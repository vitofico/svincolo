use serde_json::{Value, json};

pub(super) fn strict(schema: &Value) -> Option<Value> {
    if schema["type"] != "object" {
        return None;
    }
    let mut rewritten = schema.clone();
    rewrite(&mut rewritten, 1)?;
    Some(rewritten)
}

fn rewrite(schema: &mut Value, depth: usize) -> Option<()> {
    if depth > 10 {
        return None;
    }
    let fields = schema.as_object_mut()?;
    let types = fields.get("type")?;
    let valid_type = |kind: &Value| {
        matches!(
            kind.as_str(),
            Some("object" | "array" | "string" | "number" | "integer" | "boolean" | "null")
        )
    };
    if !valid_type(types)
        && !types.as_array().is_some_and(|types| {
            types.len() == 2
                && types.iter().all(valid_type)
                && types.iter().any(|kind| kind == "null")
        })
    {
        return None;
    }

    let object = has_type(types, "object");
    let array = has_type(types, "array");
    if fields.keys().any(|key| match key.as_str() {
        "type" | "enum" | "const" | "title" | "description" => false,
        "properties" | "additionalProperties" | "required" => !object,
        "items" => !array,
        _ => true,
    }) {
        return None;
    }

    if object {
        if fields.get("additionalProperties") != Some(&Value::Bool(false)) {
            return None;
        }
        let required = fields.remove("required");
        let required = required.as_ref().and_then(Value::as_array);
        let properties = fields.get_mut("properties")?.as_object_mut()?;
        if required.is_some_and(|names| {
            names.iter().any(|name| {
                !name
                    .as_str()
                    .is_some_and(|name| properties.contains_key(name))
            })
        }) {
            return None;
        }
        for (name, child) in properties.iter_mut() {
            let nullable = allows_null(child);
            rewrite(child, depth + 1)?;
            if !required.is_some_and(|names| names.iter().any(|item| item == name.as_str()))
                && !nullable
            {
                *child = json!({"anyOf":[child.take(), {"type":"null"}]});
            }
        }
        let names = json!(properties.keys().collect::<Vec<_>>());
        fields.insert("required".into(), names);
    }
    if array {
        rewrite(fields.get_mut("items")?, depth + 1)?;
    }
    Some(())
}

fn has_type(types: &Value, name: &str) -> bool {
    types == name
        || types
            .as_array()
            .is_some_and(|items| items.iter().any(|item| item == name))
}

fn allows_null(schema: &Value) -> bool {
    has_type(&schema["type"], "null")
        && schema.get("const").is_none_or(Value::is_null)
        && schema["enum"]
            .as_array()
            .is_none_or(|items| items.iter().any(Value::is_null))
}

pub(super) fn restore_optional(schema: &Value, instance: &mut Value) {
    if let (Some(properties), Some(values)) =
        (schema["properties"].as_object(), instance.as_object_mut())
    {
        let required = schema["required"].as_array();
        values.retain(|name, value| {
            let Some(child) = properties.get(name) else {
                return true;
            };
            if value.is_null()
                && !required.is_some_and(|names| names.iter().any(|item| item == name.as_str()))
                && !allows_null(child)
            {
                return false;
            }
            restore_optional(child, value);
            true
        });
    } else if let (Some(items), Some(values)) = (schema.get("items"), instance.as_array_mut()) {
        for value in values {
            restore_optional(items, value);
        }
    }
}
