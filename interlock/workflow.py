"""Declarative DAG definition: Workflow/Task builder with validation.

Store.create_run() has always accepted a raw list of task dicts — that
still works, and this module doesn't change it. What's new is a way to
build that list that validates the DAG *before* anything touches the
database: a task referencing a name that doesn't exist, or a dependency
cycle, previously meant a run that silently sat `pending` forever (nothing
in Store ever detects "no task can ever become ready" — it just never
promotes anything). Catching both at definition time turns that into an
immediate, readable error instead of a run that looks alive but is dead.
"""

from __future__ import annotations

from dataclasses import dataclass, field


class WorkflowError(ValueError):
    """The DAG definition itself is invalid — raised at build time, never
    something a worker or the scheduler discovers at run time."""


@dataclass(frozen=True)
class Task:
    name: str
    command: list[str]
    needs: tuple[str, ...] = ()
    max_retries: int = 3


class Workflow:
    """Fluent builder. Usage::

        wf = (
            Workflow()
            .task("fetch", command=["python3", "fetch.py"])
            .task("parse", command=["python3", "parse.py"], needs=["fetch"])
            .task("report", command=["python3", "report.py"], needs=["parse"])
        )
        wf.validate()  # raises WorkflowError if malformed; also called by tasks()
        store.create_run(run_id, wf.tasks())
    """

    def __init__(self):
        self._tasks: dict[str, Task] = {}

    def task(
        self,
        name: str,
        command: list[str],
        *,
        needs: list[str] | tuple[str, ...] = (),
        max_retries: int = 3,
    ) -> "Workflow":
        if not name:
            raise WorkflowError("task name must be non-empty")
        if name in self._tasks:
            raise WorkflowError(f"duplicate task name: {name!r}")
        self._tasks[name] = Task(name=name, command=list(command), needs=tuple(needs), max_retries=max_retries)
        return self

    def validate(self) -> None:
        """Raises WorkflowError on: a `needs` reference to an undefined task,
        a dependency cycle, or an empty workflow. Cycle detection is a plain
        DFS coloring walk (white/gray/black) — fine at this scale; this runs
        once at definition time, never in the hot path."""
        if not self._tasks:
            raise WorkflowError("workflow has no tasks")
        for task in self._tasks.values():
            for dep in task.needs:
                if dep not in self._tasks:
                    raise WorkflowError(f"task {task.name!r} needs undefined task {dep!r}")

        WHITE, GRAY, BLACK = 0, 1, 2
        color = {name: WHITE for name in self._tasks}

        def visit(name: str, path: list[str]) -> None:
            color[name] = GRAY
            path.append(name)
            for dep in self._tasks[name].needs:
                if color[dep] == GRAY:
                    cycle = path[path.index(dep):] + [dep]
                    raise WorkflowError(f"dependency cycle: {' -> '.join(cycle)}")
                if color[dep] == WHITE:
                    visit(dep, path)
            path.pop()
            color[name] = BLACK

        for name in self._tasks:
            if color[name] == WHITE:
                visit(name, [])

    def tasks(self) -> list[dict]:
        """Validates, then returns the raw dict list Store.create_run expects."""
        self.validate()
        return [
            {"name": t.name, "command": t.command, "needs": list(t.needs), "max_retries": t.max_retries}
            for t in self._tasks.values()
        ]

    def __len__(self) -> int:
        return len(self._tasks)
