from .context import Context


class JsonLogic:
    def __init__(self, condition: dict):
        self.condition = condition

    def evaluate(self, context: Context) -> bool:
        return self._eval(self.condition, {"vars": context.vars})

    def _eval(self, expr, data):
        if isinstance(expr, dict):
            if len(expr) != 1:
                raise ValueError("Invalid JSONLogic expression")
            op, args = next(iter(expr.items()))
            if not isinstance(args, list):
                args = [args]

            if op == "var":
                path = args[0].split(".")
                val = data
                for p in path:
                    if val is None:
                        return None
                    val = val.get(p, None)
                return val
            elif op == "<":
                return self._eval(args[0], data) < self._eval(args[1], data)
            elif op == ">":
                return self._eval(args[0], data) > self._eval(args[1], data)
            elif op == "==":
                return self._eval(args[0], data) == self._eval(args[1], data)
            elif op == "and":
                return all(self._eval(a, data) for a in args)
            elif op == "or":
                return any(self._eval(a, data) for a in args)
            else:
                raise NotImplementedError(f"Operator {op} not implemented")
        else:
            return expr
