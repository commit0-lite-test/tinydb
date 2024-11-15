from typing import TypeVar, Optional, Callable, Dict
from mypy.nodes import NameExpr
from mypy.options import Options
from mypy.plugin import Plugin, DynamicClassDefContext
T = TypeVar('T')
CB = Optional[Callable[[T], None]]
DynamicClassDef = DynamicClassDefContext

class TinyDBPlugin(Plugin):

    def __init__(self, options: Options):
        super().__init__(options)
        self.named_placeholders: Dict[str, str] = {}

    def get_dynamic_class_hook(self, fullname: str) -> CB[DynamicClassDef]:
        if fullname == 'tinydb.utils.with_typehint':
            return self.with_typehint_callback
        return None

    def with_typehint_callback(self, ctx: DynamicClassDef) -> None:
        if len(ctx.call.args) != 1:
            ctx.api.fail("with_typehint() requires exactly one argument", ctx.call)
            return

        base_type = ctx.api.analyze_type(ctx.call.args[0])
        if isinstance(base_type, NameExpr):
            ctx.class_def.base_type_exprs.append(base_type)
