import os
import platform
import sys

try:
    import psutil

    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False


def get_system_hardware_info() -> dict[str, str]:
    """Retrieve detailed OS, CPU, memory, and accelerator platform info."""
    os_info = f"{platform.system()} {platform.release()} ({platform.machine()})"
    python_ver = sys.version.split()[0]
    cpu_info = platform.processor() or platform.machine()
    logical_cores = psutil.cpu_count(logical=True) if HAS_PSUTIL else (os.cpu_count() or 1)
    physical_cores = psutil.cpu_count(logical=False) if HAS_PSUTIL else logical_cores
    ram_gb = f"{round(psutil.virtual_memory().total / (1024**3), 2)} GB" if HAS_PSUTIL else "N/A"

    # Detect Accelerator / Platform Memory Architecture
    device_class = "CPU / Host DRAM"
    arch_type = "Standard NUMA / Host Memory"

    if platform.system() == "Darwin" and platform.machine() == "arm64":
        device_class = "Apple Silicon Metal MPS"
        arch_type = "Unified Memory Architecture (UMA)"
    else:
        try:
            import torch

            if torch.cuda.is_available():
                device_class = f"NVIDIA CUDA ({torch.cuda.get_device_name(0)})"
                arch_type = "High Bandwidth Memory (HBM/NVLink)"
        except ImportError:
            pass

    return {
        "os": os_info,
        "python_version": python_ver,
        "cpu": f"{cpu_info} ({physical_cores} Cores / {logical_cores} Threads)",
        "ram_gb": f"{ram_gb} GB",
        "device_class": device_class,
        "memory_architecture": arch_type,
    }
