from typing import TypeVar, Optional, Callable, Dict, Any, TYPE_CHECKING
from collections import defaultdict

T = TypeVar("T")
CB = Optional[Callable[[T], None]]

if TYPE_CHECKING:
    from mypy.nodes import NameExpr
    from mypy.options import Options
    from mypy.plugin import Plugin, DynamicClassDefContext

    DynamicClassDef = DynamicClassDefContext
else:
    NameExpr = Options = Plugin = DynamicClassDefContext = Any
    DynamicClassDef = Any


class TinyDBPlugin(Plugin):
    def __init__(self, options: Options):
        super().__init__(options)
        self.dynamic_class_hooks: Dict[str, Optional[Callable[[Any], None]]] = defaultdict(lambda: None)
        self.dynamic_class_hooks["tinydb.utils.with_typehint"] = self.with_typehint_callback

    def get_dynamic_class_hook(
        self, fullname: str
    ) -> Optional[Callable[[Any], None]]:
        """Get the dynamic class hook for the given fullname.

        Args:
        ----
            fullname (str): The full name of the class.

        Returns:
        -------
            Optional[Callable[[Any], None]]: The callback function if the fullname matches, None otherwise.

        """
        return self.dynamic_class_hooks[fullname]

    def with_typehint_callback(self, ctx: Any) -> None:
        """Callback function for the with_typehint dynamic class.

        Args:
        ----
            ctx (Any): The dynamic class definition context.

        """
        if len(ctx.call.args) != 1:
            ctx.api.fail("with_typehint() requires exactly one argument", ctx.call)

        base_type = ctx.api.analyze_type(ctx.call.args[0])
        if isinstance(base_type, NameExpr):
            ctx.class_def.base_type_exprs.append(base_type)
