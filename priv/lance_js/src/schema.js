const arrow = require("apache-arrow");

function cfgToField(cfg) {
  const field = function (name, type, nullable) {
    return [new arrow.Field(name, type, nullable)];
  };
  const { name, field_type, nullable } = cfg;
  switch (field_type.type) {
    case "utf8":
      return field(name, new arrow.Utf8(), nullable);
    case "int32":
      return field(name, new arrow.Int32(), nullable);
    case "float32":
      return field(name, new arrow.Float32(), nullable);
    case "list":
      return field(name, cfgToField(field_type.child), nullable);
    case "fixed_size_list":
      return field(name, cfgToField(field_type.child), nullable);

    default:
      return [];
  }
}

function fieldConfigsToSchema(fieldConfigs) {
  return new arrow.Schema([].concat(fieldConfigs.map(cfgToField)[0]));
}

module.exports = {
  fieldConfigsToSchema,
};
