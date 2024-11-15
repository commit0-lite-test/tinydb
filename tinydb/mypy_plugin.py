from typing import TypeVar, Optional, Callable, Dict, TYPE_CHECKING

T = TypeVar("T")
CB = Optional[Callable[[T], None]]

if TYPE_CHECKING:
    from mypy.nodes import NameExpr
    from mypy.options import Options
    from mypy.plugin import Plugin, DynamicClassDefContext
    DynamicClassDef = DynamicClassDefContext
else:
    NameExpr = Options = Plugin = DynamicClassDefContext = object
    DynamicClassDef = object


class TinyDBPlugin(Plugin):
    def __init__(self, options: Options):
        super().__init__(options)
        self.named_placeholders: Dict[str, str] = {}

    def get_dynamic_class_hook(self, fullname: str) -> Optional[Callable[[DynamicClassDef], None]]:
        """
        Get the dynamic class hook for the given fullname.

        Args:
            fullname (str): The full name of the class.

        Returns:
            Optional[Callable[[DynamicClassDef], None]]: The callback function if the fullname matches, None otherwise.
        """
        if fullname == "tinydb.utils.with_typehint":
            return self.with_typehint_callback
        return None

    def with_typehint_callback(self, ctx: DynamicClassDef) -> None:
        """
        Callback function for the with_typehint dynamic class.

        Args:
            ctx (DynamicClassDef): The dynamic class definition context.
        """
        if len(ctx.call.args) != 1:
            ctx.api.fail("with_typehint() requires exactly one argument", ctx.call)
            return

        base_type = ctx.api.analyze_type(ctx.call.args[0])
        if isinstance(base_type, NameExpr):
            ctx.class_def.base_type_exprs.append(base_type)
