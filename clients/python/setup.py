import os

from setuptools import Distribution, setup
from wheel.bdist_wheel import bdist_wheel


class NativeDistribution(Distribution):
    def has_ext_modules(self):
        return True


class Py3PlatformWheel(bdist_wheel):
    def get_tag(self):
        platform_tag = os.environ.get("DEOOS_WHEEL_PLATFORM")
        if platform_tag:
            return "py3", "none", platform_tag
        _, _, platform_tag = super().get_tag()
        return "py3", "none", platform_tag


setup(distclass=NativeDistribution, cmdclass={"bdist_wheel": Py3PlatformWheel})
