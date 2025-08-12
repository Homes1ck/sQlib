import joblib
from joblib import Parallel, delayed
from joblib._parallel_backends import MultiprocessingBackend

class ParallelExt(Parallel):
    def __init__(self, *args, **kwargs):
        maxtasksperchild = kwargs.pop("maxtasksperchild", None)
        super(ParallelExt, self).__init__(*args, **kwargs)
        if isinstance(self._backend, MultiprocessingBackend):
            # 2025-05-04 joblib released version 1.5.0, in which _backend_args was removed and replaced by _backend_kwargs.
            # Ref: https://github.com/joblib/joblib/pull/1525/files#diff-e4dff8042ce45b443faf49605b75a58df35b8c195978d4a57f4afa695b406bdc
            if joblib.__version__ < "1.5.0":
                self._backend_args["maxtasksperchild"] = maxtasksperchild  # pylint: disable=E1101
            else:
                self._backend_kwargs["maxtasksperchild"] = maxtasksperchild  # pylint: disable=E1101
