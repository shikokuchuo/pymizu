"""Build wiring for pyrei.

The extension compiles the vendored librei core sources (PyPI-style
self-containment — no system librei is required or consulted). The vendored
TUs need no defines: REI_API defaults to empty under static linkage.

Windows builds force clang-cl: MSVC's C11 atomics are unsupported, and
setuptools' msvc backend resolves cl.exe itself, ignoring CC — so the
override goes through build_ext, not the environment.
"""

import glob
import os

from setuptools import Extension, setup
from setuptools.command.build_ext import build_ext


def _find_on_path(exe):
    """Return the absolute path of exe on PATH, or exe unchanged."""
    for d in os.environ.get("PATH", "").split(os.pathsep):
        cand = os.path.join(d, exe)
        if os.path.isfile(cand):
            return cand
    return exe


class build_ext_rei(build_ext):
    def build_extensions(self):
        if os.name == "nt":
            # setuptools' msvc backend resolves cl.exe itself and ignores CC;
            # force clang-cl (MSVC C11 atomics unsupported). initialize()
            # sets compiler.cc from the Visual Studio environment, and
            # compile() initializes only once, so the replacement sticks.
            # /GL (whole-program optimization) is cl.exe-specific.
            self.compiler.initialize(self.plat_name)
            self.compiler.cc = _find_on_path("clang-cl.exe")
            self.compiler.compile_options = [
                o for o in self.compiler.compile_options if o != "/GL"
            ]
        super().build_extensions()


core_sources = sorted(glob.glob("src/vendor/librei/*.c"))

ext_modules = [
    Extension(
        "pyrei._pyrei",
        sources=["src/_pyrei.c"] + core_sources,
        include_dirs=["src/vendor/librei"],
        extra_compile_args=[] if os.name == "nt" else ["-std=c11", "-pthread"],
        extra_link_args=[] if os.name == "nt" else ["-pthread"],
    )
]

setup(ext_modules=ext_modules, cmdclass={"build_ext": build_ext_rei})
