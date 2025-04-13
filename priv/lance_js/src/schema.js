const arrow = require("apache-arrow");

function cfgToField(cfg) {
  const field = function (name, type, nullable) {
    return [new arrow.Field(name, type, nullable)];
  };
  const { name, field_type, nullable } = cfg;
  switch (field_type.type) {
    case "utf8":
      return field(name, new arrow.Utf8(), nullable);
    case "float":
      return field(name, new arrow.Float(field_type.precision), nullable);
    case "int32":
      return field(name, new arrow.Int32(), nullable);
    case "float32":
      return field(name, new arrow.Float32(), nullable);
    case "list":
      return [
        new arrow.Field(
          name,
          new arrow.List(cfgToField(field_type.child)[0]),
          nullable
        ),
      ];
    case "fixed_size_list":
      return [
        new arrow.Field(
          name,
          new arrow.FixedSizeList(
            field_type.dimension,
            cfgToField(field_type.child)[0]
          ),
          nullable
        ),
      ];

    default:
      return [];
  }
}

function fieldConfigsToSchema(fieldConfigs) {
  const fields = fieldConfigs.map(cfgToField).flat();
  return new arrow.Schema(fields);
}

module.exports = {
  fieldConfigsToSchema,
};
