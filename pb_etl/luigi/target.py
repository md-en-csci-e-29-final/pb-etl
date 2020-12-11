from luigi.local_target import LocalTarget, atomic_file
from contextlib import contextmanager
from csci_utils.io import atomic_write


class suffix_preserving_atomic_file(atomic_file):

    def generate_tmp_path(self, path):

        with atomic_write(path, mode="w", as_file=False) as ret:
            return ret


class BaseAtomicProviderLocalTarget(LocalTarget):
    # Allow some composability of atomic handling
    atomic_provider = atomic_file

    def open(self, mode="r"):
        # leverage super() as well as modifying any code in LocalTarget
        # to use self.atomic_provider rather than atomic_file

        rwmode = mode.replace("b", "").replace("t", "")

        if rwmode == "w":
            self.makedirs()
            return self.format.pipe_writer(self.atomic_provider(self.path))
        else:
            return super().open(mode)

    @contextmanager
    def temporary_path(self):
        # NB: unclear why LocalTarget doesn't use atomic_file in its implementation
        self.makedirs()
        with self.atomic_provider(self.path) as af:
            yield af.tmp_path


class SuffixPreservingLocalTarget(BaseAtomicProviderLocalTarget):
    atomic_provider = suffix_preserving_atomic_file