import os
import subprocess
from pathlib import Path


def propagate_required_changes(clp_package_home: Path, clp_repo_root: Path) -> None:
    lib_dest = clp_package_home / "lib/python3/site-packages/."
    items_to_copy = {
        clp_repo_root / "components/job-orchestration/job_orchestration": lib_dest,
    }

    command = ["rsync", "-av"]
    for src, dest in items_to_copy.items():
        new_command = command + [src, dest]
        result = subprocess.run(new_command, capture_output=True, text=True)
        # Check the return code
        if result.returncode != 0:
            raise ValueError(f"Command {new_command} failed. Error: {result.stderr}")


# Issue: this can't be called when the patch is already applied
def patch_package(clp_package_home: Path, clp_repo_root: Path, patch_file: Path) -> None:
    patch_file = patch_file.resolve()
    if not patch_file.exists() or not patch_file.is_file():
        raise ValueError(f"Patch file {patch_file} is not a valid patch file")

    subprocess.run(["git", "apply", str(patch_file)], check=True, cwd=clp_repo_root)
    propagate_required_changes(clp_package_home, clp_repo_root)


# Issue: this can't be called when the patch is not applied
def revert_patch(clp_package_home: Path, clp_repo_root: Path, patch_file: Path) -> None:
    patch_file = patch_file.resolve()
    if not patch_file.exists() or not patch_file.is_file():
        raise ValueError(f"Patch file {patch_file} is not a valid patch file")
    subprocess.run(["git", "apply", "-R", str(patch_file)], check=True, cwd=clp_repo_root)
    propagate_required_changes(clp_package_home, clp_repo_root)
