# Expressions

The expression language is [CEL](https://github.com/google/cel-spec). An
expression is allowed in `condition` of a `condition` node, in `when` of a
`switch` node, and in any value whose key carries the `.$` suffix in
`arguments`, `outputs` and `variables`.

```json
"outputs": {
  "value": "n",
  "attempts.$": "0",
  "greeting.$": "'hello ' + string(vars.user_name)"
}
```

Frame variables are addressed through the `vars` namespace (`vars.n`). A name
that is not an ASCII identifier is addressed by index: `vars['итог']`.

A key with the `.$` suffix in `outputs` names a variable rather than a stage
field, so a single node can introduce any number of pipeline variables.

Backend: `common-expression-language` (native), falling back to `cel-python`.
