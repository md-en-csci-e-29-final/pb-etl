from functools import partial
from luigi import Task, LocalTarget
from hashlib import sha256
from luigi.task import flatten


class Requirement:
    def __init__(self, task_class, **params):
        self.task_class = task_class
        self.params = params

    def __get__(self, task, cls) -> object:

        if task is None:
            return self
        return task.clone(self.task_class, **self.params)


class Requires:
    def __get__(self, task, cls):
        self.cls = cls
        # Bind self/task in a closure
        return partial(self.__call__, task)

    def __call__(self, task):
        """Returns the requirements of a task
        Assumes the task class has :class:`.Requirement` descriptors, which
        can clone the appropriate dependences from the task instance.
        :returns: requirements compatible with `task.requires()`
        :rtype: dict
        """
        # Search task.__class__ for Requirement instances
        # return

        ret = {
            k: getattr(task, k)
            for (k, v) in self.cls.__dict__.items()
            if isinstance(v, Requirement)
        }
        if len(ret) > 1:
            return ret
        else:
            return list(ret.values())[0]


class TargetOutput:
    def __init__(
        self,
        file_pattern="{task.__class__.__name__}",
        ext=".txt",
        target_class=LocalTarget,
        **target_kwargs
    ):

        self.file_pattern = file_pattern
        self.ext = ext
        self.target_class = target_class
        self.target_kwargs = target_kwargs
        self.path = target_kwargs.pop("target_path")

    def __get__(self, task, cls):
        return partial(self.__call__, task)

    def __call__(self, task):
        # Determine the path etc here

        # path = self.target_kwargs.pop('target_path')

        # pp = self.target_kwargs.pop('path_param')

        # dir_name = os.path.dirname(pp)
        # file_name = os.path.basename(pp)
        # no_ext_fn = os.path.splitext(file_name)[0]
        #
        # path = dir_name + "/" + self.file_pattern + "-" + no_ext_fn + self.ext

        return self.target_class(
            (self.path + "-" + self.file_pattern + "/").format(task=task),
            **self.target_kwargs
        )


class SaltedOutput(TargetOutput):
    def __init__(
        self,
        file_pattern="{task.__class__.__name__}-{salt}",
        ext=".txt",
        target_class=LocalTarget,
        **target_kwargs
    ):
        super().__init__(file_pattern, ext, target_class, **target_kwargs)

    def __call__(self, task):

        return self.target_class(
            (self.path + "-" + self.file_pattern + "/").format(
                task=task, salt=get_salted_version(task)[:6]
            ),
            **self.target_kwargs
        )


def get_salted_version(task):
    """Create a salted id/version for this task and lineage
    :returns: a unique, deterministic hexdigest for this task
    :rtype: str
    """

    msg = ""

    # Salt with lineage
    for req in flatten(task.requires()):
        # Note that order is important and impacts the hash - if task
        # requirements are a dict, then consider doing this is sorted order
        msg += get_salted_version(req)

    # Uniquely specify this task
    msg += ",".join(
        [
            # Basic capture of input type
            task.__class__.__name__,
            # Change __version__ at class level when everything needs rerunning!
            task.__version__,
        ]
        + [
            # Depending on strictness - skipping params is acceptable if
            # output already is partitioned by their params; including every
            # param may make hash *too* sensitive
            "{}={}".format(param_name, repr(task.param_kwargs[param_name]))
            for param_name, param in sorted(task.get_params())
            if param.significant
        ]
    )
    return sha256(msg.encode()).hexdigest()


def salted_target(task, file_pattern, format=None, **kwargs):
    """A local target with a file path formed with a 'salt' kwarg
    :rtype: LocalTarget
    """
    return LocalTarget(
        file_pattern.format(salt=get_salted_version(task)[:6], self=task, **kwargs),
        format=format,
    )