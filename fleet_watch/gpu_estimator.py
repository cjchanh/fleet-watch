"""GPU working set estimator — prevents memory overcommit on Apple Silicon.

Computes total working set (weights + KV cache + activations + pool overhead)
for inference workloads and compares against physical RAM. Static GPU budget
accounting misses runtime buffer growth; this module catches it.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any


# Framework-specific pool overhead multipliers.
# Candle retains intermediate buffers until command buffer completion — 2.0x.
# MLX reuses aggressively — 1.3x.  llama.cpp (Ollama) — 1.1x.
FRAMEWORK_OVERHEAD: dict[str, float] = {
    "candle": 2.0,
    "mlx": 1.3,
    "ollama": 1.1,
    "llama_cpp": 1.1,
    "vllm": 1.4,
}

# Known model architectures: (param_billions, layers, hidden_dim)
# Used when command-line metadata is insufficient.
MODEL_SPECS: dict[str, tuple[float, int, int]] = {
    "0.6B": (0.6, 24, 1024),
    "1B": (1.0, 16, 2048),
    "3B": (3.0, 28, 3072),
    "7B": (7.0, 28, 4096),
    "8B": (8.0, 32, 4096),
    "9B": (9.0, 32, 4096),
    "13B": (13.0, 40, 5120),
    "14B": (14.0, 40, 5120),
    "26B": (26.0, 42, 5376),
    "32B": (32.0, 64, 5120),
    "35B": (35.0, 64, 5376),
    "70B": (70.0, 80, 8192),
    "122B": (122.0, 96, 12288),
}

# Ollama and common model tag aliases that don't follow the NB convention.
# Maps tag fragments to total parameter sizes (not effective/active params).
# Gemma 4 E4B is 9B total params with 4B effective (MoE); memory use is 9B.
# Gemma 4 E2B is 9B total params with 2B effective (PLE); memory use is 9B.
MODEL_TAG_ALIASES: dict[str, str] = {
    "e4b": "9B",     # Gemma 4 E4B — 9B total, 4B effective
    "e2b": "9B",     # Gemma 4 E2B — 9B total, 2B effective
    "gemma4:e4b": "9B",
    "gemma4:e2b": "9B",
    "gemma4:latest": "9B",
    "gemma4:26b": "26B",
    "gemma4:31b": "32B",  # Gemma 4 31B maps to 32B spec (closest)
    "gemma3:4b": "3B",
    "phi-4": "14B",
    "phi4": "14B",
}

# Quantization: bytes per parameter
QUANT_BYTES: dict[str, float] = {
    "f32": 4.0,
    "f16": 2.0,
    "bf16": 2.0,
    "8bit": 1.0,
    "q8": 1.0,
    "q6_k": 0.75,
    "q5_k_m": 0.625,
    "q5_k": 0.625,
    "q4_k_m": 0.5,
    "q4_k": 0.5,
    "q4_0": 0.5,
    "q3_k": 0.375,
    "q2_k": 0.25,
    "4bit": 0.5,
}

DEFAULT_QUANT = "q4_k_m"
DEFAULT_MAX_SEQ = 4096
DEFAULT_HEADROOM_MB = 2048


@dataclass
class WorkingSetEstimate:
    """Breakdown of estimated GPU memory consumption."""
    weights_mb: int
    kv_cache_mb: int
    activations_mb: int
    overhead_multiplier: float
    total_mb: int
    framework: str
    model_size: str
    quantization: str
    physical_ram_mb: int
    available_after_reserve_mb: int
    fits: bool
    grounded: bool
    source: str
    suggestion: str | None

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "weights_mb": self.weights_mb,
            "kv_cache_mb": self.kv_cache_mb,
            "activations_mb": self.activations_mb,
            "overhead_multiplier": self.overhead_multiplier,
            "total_mb": self.total_mb,
            "framework": self.framework,
            "model_size": self.model_size,
            "quantization": self.quantization,
            "physical_ram_mb": self.physical_ram_mb,
            "available_after_reserve_mb": self.available_after_reserve_mb,
            "fits": self.fits,
            "grounded": self.grounded,
            "source": self.source,
        }
        if self.suggestion:
            d["suggestion"] = self.suggestion
        return d


def detect_framework(command: str) -> str:
    """Infer inference framework from a process command line."""
    cmd_lower = command.lower()
    if "candle" in cmd_lower or "cake" in cmd_lower:
        return "candle"
    if (
        "mlx_lm" in cmd_lower
        or "mlx_vlm" in cmd_lower
        or re.search(r"\bpython\d*\s+-m\s+mlx\b", cmd_lower)
    ):
        return "mlx"
    if "ollama" in cmd_lower:
        return "ollama"
    if "llama_cpp" in cmd_lower or "llama-cpp" in cmd_lower or "llama.cpp" in cmd_lower:
        return "llama_cpp"
    if "vllm" in cmd_lower:
        return "vllm"
    return "unknown"


def detect_model_size(command: str) -> str | None:
    """Extract model size like '7B' from a command line or model path.

    Checks MODEL_TAG_ALIASES first for Ollama-style tags (e4b, e2b, etc.)
    that don't follow the standard NB convention.
    """
    cmd_lower = command.lower()

    # Check tag aliases first (exact and substring match)
    for tag, size in sorted(MODEL_TAG_ALIASES.items(), key=lambda x: len(x[0]), reverse=True):
        if tag in cmd_lower:
            return size

    # Standard NB pattern
    match = re.search(r"(\d+)[Bb]", command)
    if match:
        return f"{match.group(1)}B"
    return None


def detect_quantization(command: str) -> str:
    """Extract quantization format from a command line or model path."""
    cmd_lower = command.lower()
    # Check specific quant formats in order (most specific first)
    for quant in sorted(QUANT_BYTES.keys(), key=len, reverse=True):
        pattern = quant.replace("_", "[_-]?")
        if re.search(pattern, cmd_lower):
            return quant
    # Common naming conventions
    if "8bit" in cmd_lower or "8-bit" in cmd_lower:
        return "8bit"
    if "4bit" in cmd_lower or "4-bit" in cmd_lower:
        return "4bit"
    return DEFAULT_QUANT


def estimate_weights_mb(params_b: float, quant: str) -> int:
    """Estimate model weight size in MB."""
    bytes_per_param = QUANT_BYTES.get(quant, 0.5)
    return int(params_b * 1e9 * bytes_per_param / (1024 * 1024))


def estimate_kv_cache_mb(
    layers: int,
    hidden_dim: int,
    max_seq: int = DEFAULT_MAX_SEQ,
) -> int:
    """Estimate KV cache size in MB.

    KV cache = 2 (K+V) * layers * hidden_dim * max_seq * 2 bytes (F16)
    """
    kv_bytes = 2 * layers * hidden_dim * max_seq * 2
    return int(kv_bytes / (1024 * 1024))


def estimate_activations_mb(hidden_dim: int, intermediate_factor: int = 4) -> int:
    """Estimate peak activation memory per forward pass.

    Lightweight heuristic for decode-time transient tensors. The exact live set
    varies by framework and scheduling strategy, so this keeps a floor estimate
    and leaves the heavier decode pressure to the framework overhead multiplier.
    """
    act_bytes = hidden_dim * intermediate_factor * 2 * 1024
    return max(int(act_bytes / (1024 * 1024)), 64)


def resolve_effective_reserve_mb(
    physical_ram_mb: int,
    configured_reserve_mb: int | None = None,
) -> int:
    """Resolve a physical-RAM headroom that is sane for the current machine.

    Fleet's registry reserve is a logical GPU-budget setting. On smaller machines
    the global default reserve can exceed physical RAM, so estimator enforcement
    needs a machine-aware clamp instead of blindly using that value.
    """
    if physical_ram_mb <= 0:
        return max(0, configured_reserve_mb or DEFAULT_HEADROOM_MB)

    if configured_reserve_mb is None:
        return min(DEFAULT_HEADROOM_MB, max(physical_ram_mb - 1, 0))

    if 0 <= configured_reserve_mb < physical_ram_mb:
        return configured_reserve_mb

    return min(DEFAULT_HEADROOM_MB, max(physical_ram_mb - 1, 0))


def estimate_working_set(
    *,
    framework: str | None = None,
    model_size: str | None = None,
    quantization: str | None = None,
    command: str | None = None,
    max_seq: int = DEFAULT_MAX_SEQ,
    physical_ram_mb: int = 0,
    reserve_mb: int = DEFAULT_HEADROOM_MB,
    config_overrides: dict[str, Any] | None = None,
    allow_model_fallback: bool = True,
) -> WorkingSetEstimate:
    """Estimate total GPU working set for an inference workload.

    Can infer framework, model size, and quantization from a command line,
    or accept them explicitly.
    """
    overrides = config_overrides or {}
    overhead_map = {**FRAMEWORK_OVERHEAD, **overrides.get("framework_overhead", {})}

    fw = framework or (detect_framework(command) if command else "unknown")
    ms = model_size or (detect_model_size(command) if command else None)
    qt = quantization or (detect_quantization(command) if command else DEFAULT_QUANT)
    source = "explicit" if framework or model_size or quantization else "command"
    grounded = fw != "unknown" and ms is not None

    if ms and ms.upper() in MODEL_SPECS:
        params_b, layers, hidden_dim = MODEL_SPECS[ms.upper()]
    elif ms:
        # Attempt to parse just the size
        match = re.match(r"(\d+)", ms)
        if match:
            size_b = float(match.group(1))
            # Rough estimates for unknown architectures
            layers = max(16, int(size_b * 4))
            hidden_dim = max(2048, int(size_b ** 0.5 * 1600))
            params_b = size_b
        else:
            params_b, layers, hidden_dim = 7.0, 28, 4096
            grounded = False
            source = "fallback_default"
    else:
        grounded = False
        if not allow_model_fallback:
            return WorkingSetEstimate(
                weights_mb=0,
                kv_cache_mb=0,
                activations_mb=0,
                overhead_multiplier=0.0,
                total_mb=0,
                framework=fw,
                model_size="UNKNOWN",
                quantization=qt,
                physical_ram_mb=physical_ram_mb,
                available_after_reserve_mb=max(0, physical_ram_mb - reserve_mb) if physical_ram_mb > 0 else 0,
                fits=True,
                grounded=False,
                source="insufficient_input",
                suggestion=None,
            )
        params_b, layers, hidden_dim = 7.0, 28, 4096
        ms = "7B"
        source = "fallback_default"

    weights = estimate_weights_mb(params_b, qt)
    kv_cache = estimate_kv_cache_mb(layers, hidden_dim, max_seq)
    activations = estimate_activations_mb(hidden_dim)
    overhead = overhead_map.get(fw, 1.5)

    # Framework pool pressure applies primarily to transient tensors, not the
    # static model weights. Multiplying only the transient slice avoids
    # over-denying larger models that fit their weight residency but still need
    # decode-time scratch buffers.
    transient_mb = kv_cache + activations
    total_mb = int(weights + transient_mb * overhead)

    available = physical_ram_mb - reserve_mb if physical_ram_mb > 0 else 0
    fits = total_mb <= available if physical_ram_mb > 0 else True

    suggestion = None
    if not fits and physical_ram_mb > 0:
        suggestion = _suggest_alternative(
            params_b=params_b,
            qt=qt,
            available_mb=available,
            overhead=overhead,
            layers=layers,
            hidden_dim=hidden_dim,
            max_seq=max_seq,
        )

    return WorkingSetEstimate(
        weights_mb=weights,
        kv_cache_mb=kv_cache,
        activations_mb=activations,
        overhead_multiplier=overhead,
        total_mb=total_mb,
        framework=fw,
        model_size=ms.upper() if ms else "UNKNOWN",
        quantization=qt,
        physical_ram_mb=physical_ram_mb,
        available_after_reserve_mb=max(0, available),
        fits=fits,
        grounded=grounded,
        source=source,
        suggestion=suggestion,
    )


def _suggest_alternative(
    *,
    params_b: float,
    qt: str,
    available_mb: int,
    overhead: float,
    layers: int,
    hidden_dim: int,
    max_seq: int,
) -> str:
    """Suggest a quantization or model that fits."""
    kv = estimate_kv_cache_mb(layers, hidden_dim, max_seq)
    act = estimate_activations_mb(hidden_dim)

    # Try smaller quantizations
    for smaller_qt in ["q4_k_m", "q3_k", "q2_k"]:
        if QUANT_BYTES.get(smaller_qt, 1.0) >= QUANT_BYTES.get(qt, 0.5):
            continue
        w = estimate_weights_mb(params_b, smaller_qt)
        total = int(w + (kv + act) * overhead)
        if total <= available_mb:
            return (
                f"Use {smaller_qt} quantization (~{w} MB weights, "
                f"~{total} MB working set)"
            )

    # Try smaller models
    for smaller_size, (sp, sl, sh) in sorted(
        MODEL_SPECS.items(), key=lambda x: x[1][0],
    ):
        w = estimate_weights_mb(sp, qt)
        sk = estimate_kv_cache_mb(sl, sh, max_seq)
        sa = estimate_activations_mb(sh)
        total = int(w + (sk + sa) * overhead)
        if total <= available_mb:
            return (
                f"Use {smaller_size} model (~{total} MB working set) "
                f"or offload to a larger machine"
            )

    return "Offload to a machine with more RAM"
