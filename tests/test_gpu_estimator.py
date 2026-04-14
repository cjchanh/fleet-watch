"""Tests for GPU working set estimator."""

from fleet_watch import gpu_estimator


# --- Framework detection ---

def test_detect_candle():
    assert gpu_estimator.detect_framework("./target/release/cake --model 7B") == "candle"


def test_detect_mlx():
    assert gpu_estimator.detect_framework("python3 -m mlx_lm server --model foo") == "mlx"


def test_detect_framework_does_not_treat_model_repo_name_as_runtime():
    assert gpu_estimator.detect_framework("mlx-community/qwen-7B-4bit") == "unknown"


def test_detect_ollama():
    assert gpu_estimator.detect_framework("ollama serve") == "ollama"


def test_detect_vllm():
    assert gpu_estimator.detect_framework("python3 -m vllm.entrypoints.openai") == "vllm"


def test_detect_llama_cpp():
    assert gpu_estimator.detect_framework("./llama.cpp/build/bin/server") == "llama_cpp"


def test_detect_unknown():
    assert gpu_estimator.detect_framework("python3 myscript.py") == "unknown"


# --- Model size detection ---

def test_detect_model_size_7b():
    assert gpu_estimator.detect_model_size("--model qwen2.5-7B-instruct") == "7B"


def test_detect_model_size_122b():
    assert gpu_estimator.detect_model_size("mlx-community/qwen-122B-4bit") == "122B"


def test_detect_model_size_none():
    assert gpu_estimator.detect_model_size("--model custom-model") is None


# --- Ollama tag alias resolution ---

def test_detect_model_size_gemma4_e4b():
    assert gpu_estimator.detect_model_size("gemma4:e4b") == "9B"


def test_detect_model_size_gemma4_e2b():
    assert gpu_estimator.detect_model_size("gemma4:e2b") == "9B"


def test_detect_model_size_gemma4_latest():
    assert gpu_estimator.detect_model_size("gemma4:latest") == "9B"


def test_detect_model_size_gemma4_26b():
    assert gpu_estimator.detect_model_size("gemma4:26b") == "26B"


def test_detect_model_size_gemma4_31b():
    assert gpu_estimator.detect_model_size("gemma4:31b") == "32B"


def test_ollama_tag_and_explicit_size_produce_same_estimate():
    """Regression: gemma4:e4b and gemma4-9B-4bit must produce the same working set."""
    tag_est = gpu_estimator.estimate_working_set(
        framework="ollama", command="gemma4:e4b",
        physical_ram_mb=8192, reserve_mb=2048,
    )
    explicit_est = gpu_estimator.estimate_working_set(
        framework="ollama", model_size="9B", quantization="4bit",
        physical_ram_mb=8192, reserve_mb=2048,
    )
    assert tag_est.model_size == explicit_est.model_size == "9B"
    assert tag_est.total_mb == explicit_est.total_mb
    assert tag_est.fits == explicit_est.fits


def test_gemma4_e4b_does_not_fit_8gb():
    """Gemma 4 E4B (9B total) should NOT fit on 8 GB with Ollama."""
    est = gpu_estimator.estimate_working_set(
        framework="ollama", command="gemma4:e4b",
        physical_ram_mb=8192, reserve_mb=2048,
    )
    assert est.fits is False, f"Expected deny on 8GB, got total={est.total_mb}MB"
    assert est.total_mb > 6000


# --- Quantization detection ---

def test_detect_quant_q4_k_m():
    assert gpu_estimator.detect_quantization("model-Q4_K_M.gguf") == "q4_k_m"


def test_detect_quant_8bit():
    assert gpu_estimator.detect_quantization("mlx-community/foo-8bit") == "8bit"


def test_detect_quant_f16():
    assert gpu_estimator.detect_quantization("model-f16.bin") == "f16"


def test_detect_quant_default():
    assert gpu_estimator.detect_quantization("some-model") == gpu_estimator.DEFAULT_QUANT


# --- Weight estimation ---

def test_weights_7b_q4():
    mb = gpu_estimator.estimate_weights_mb(7.0, "q4_k_m")
    # 7B * 0.5 bytes = 3.5 GB ≈ 3338 MB
    assert 3000 < mb < 4000


def test_weights_7b_f16():
    mb = gpu_estimator.estimate_weights_mb(7.0, "f16")
    # 7B * 2 bytes = 14 GB ≈ 13351 MB
    assert 13000 < mb < 14000


# --- KV cache estimation ---

def test_kv_cache_7b():
    mb = gpu_estimator.estimate_kv_cache_mb(28, 4096, 4096)
    # 2 * 28 * 4096 * 4096 * 2 = ~1.75 GB
    assert 500 < mb < 2500


# --- Activation estimation ---

def test_activations_7b():
    mb = gpu_estimator.estimate_activations_mb(4096)
    assert mb >= 64


# --- Full working set estimation ---

def test_estimate_candle_7b_on_8gb():
    """The exact scenario that burned 6 hours: Candle 7B on M1 Air 8 GB."""
    est = gpu_estimator.estimate_working_set(
        framework="candle",
        model_size="7B",
        quantization="q4_k_m",
        physical_ram_mb=8192,
        reserve_mb=2048,
    )
    assert est.framework == "candle"
    assert est.model_size == "7B"
    assert est.overhead_multiplier == 2.0
    assert est.grounded is True
    # Total should exceed 6 GB available (8 GB - 2 GB reserve)
    assert est.total_mb > 6144, f"Expected >6144 MB, got {est.total_mb}"
    assert est.fits is False
    assert est.suggestion is not None


def test_estimate_candle_7b_on_128gb():
    """Same model on M5 Max 128 GB — should fit easily."""
    est = gpu_estimator.estimate_working_set(
        framework="candle",
        model_size="7B",
        quantization="q4_k_m",
        physical_ram_mb=131072,
        reserve_mb=2048,
    )
    assert est.fits is True
    assert est.suggestion is None


def test_estimate_ollama_7b_on_8gb():
    """Ollama 7B on 8 GB — lower overhead, should fit."""
    est = gpu_estimator.estimate_working_set(
        framework="ollama",
        model_size="7B",
        quantization="q4_k_m",
        physical_ram_mb=8192,
        reserve_mb=2048,
    )
    assert est.overhead_multiplier == 1.1
    assert est.fits is True


def test_estimate_mlx_26b_on_128gb():
    """MLX 26B 8-bit on 128 GB M5 — fits."""
    est = gpu_estimator.estimate_working_set(
        framework="mlx",
        model_size="26B",
        quantization="8bit",
        physical_ram_mb=131072,
        reserve_mb=2048,
    )
    assert est.fits is True


def test_estimate_from_command():
    """Infer everything from a command line."""
    est = gpu_estimator.estimate_working_set(
        command="./target/release/cake --model qwen2.5-7B-Q4_K_M.gguf",
        physical_ram_mb=8192,
        reserve_mb=2048,
    )
    assert est.framework == "candle"
    assert est.model_size == "7B"
    assert est.quantization == "q4_k_m"
    assert est.grounded is True
    assert est.fits is False


def test_estimate_no_physical_ram():
    """Without physical RAM info, fits defaults to True (can't check)."""
    est = gpu_estimator.estimate_working_set(
        framework="candle",
        model_size="7B",
    )
    assert est.fits is True
    assert est.grounded is True


def test_estimate_without_model_is_advisory_by_default():
    est = gpu_estimator.estimate_working_set(
        framework="candle",
        physical_ram_mb=8192,
        reserve_mb=2048,
    )
    assert est.grounded is False
    assert est.source == "fallback_default"


def test_estimate_without_model_can_skip_fallback():
    est = gpu_estimator.estimate_working_set(
        framework="candle",
        physical_ram_mb=8192,
        reserve_mb=2048,
        allow_model_fallback=False,
    )
    assert est.grounded is False
    assert est.source == "insufficient_input"
    assert est.total_mb == 0


def test_estimate_122b_on_128gb():
    """122B on 128 GB — should be tight with Candle overhead."""
    est = gpu_estimator.estimate_working_set(
        framework="candle",
        model_size="122B",
        quantization="q4_k_m",
        physical_ram_mb=131072,
        reserve_mb=2048,
    )
    # 122B * 0.5 bytes = ~58 GB weights alone. × 2.0 overhead → >100 GB
    assert est.weights_mb > 50000


def test_suggestion_recommends_smaller_quant():
    """When model doesn't fit, suggestion should recommend smaller quantization."""
    est = gpu_estimator.estimate_working_set(
        framework="candle",
        model_size="7B",
        quantization="q4_k_m",
        physical_ram_mb=8192,
        reserve_mb=2048,
    )
    assert est.suggestion is not None
    # Should suggest q3_k or q2_k or offload
    assert any(x in est.suggestion.lower() for x in ["q3", "q2", "offload", "smaller"])


def test_to_dict_fields():
    est = gpu_estimator.estimate_working_set(
        framework="mlx",
        model_size="7B",
        physical_ram_mb=32768,
    )
    d = est.to_dict()
    assert "weights_mb" in d
    assert "kv_cache_mb" in d
    assert "activations_mb" in d
    assert "overhead_multiplier" in d
    assert "total_mb" in d
    assert "framework" in d
    assert "fits" in d
    assert "physical_ram_mb" in d


def test_config_overrides():
    """Custom framework overhead via config."""
    est = gpu_estimator.estimate_working_set(
        framework="candle",
        model_size="7B",
        physical_ram_mb=131072,
        config_overrides={"framework_overhead": {"candle": 1.0}},
    )
    assert est.overhead_multiplier == 1.0


def test_resolve_effective_reserve_clamps_impossible_global_default():
    assert gpu_estimator.resolve_effective_reserve_mb(8192, 16384) == 2048


def test_resolve_effective_reserve_preserves_valid_configured_reserve():
    assert gpu_estimator.resolve_effective_reserve_mb(131072, 16384) == 16384


# --- Known device × model matrix ---

def test_matrix_m1_air_8gb():
    """M1 Air (8 GB) should DENY candle 7B, ALLOW ollama 3B."""
    for fw, ms, expected_fits in [
        ("candle", "7B", False),
        ("ollama", "3B", True),
        ("mlx", "3B", True),
        ("candle", "3B", True),  # 3B with candle 2x overhead ≈ 5.7 GB, fits in 6 GB
    ]:
        est = gpu_estimator.estimate_working_set(
            framework=fw,
            model_size=ms,
            quantization="q4_k_m",
            physical_ram_mb=8192,
            reserve_mb=2048,
        )
        assert est.fits is expected_fits, (
            f"{fw} {ms} on 8GB: expected fits={expected_fits}, "
            f"got fits={est.fits} (total={est.total_mb}MB)"
        )


def test_matrix_m5_max_128gb():
    """M5 Max (128 GB) should ALLOW everything up to 70B."""
    for fw, ms in [
        ("candle", "7B"),
        ("candle", "14B"),
        ("mlx", "26B"),
        ("ollama", "70B"),
    ]:
        est = gpu_estimator.estimate_working_set(
            framework=fw,
            model_size=ms,
            quantization="q4_k_m",
            physical_ram_mb=131072,
            reserve_mb=2048,
        )
        assert est.fits is True, f"{fw} {ms} on 128GB should fit"
