const dynamoose = require("dynamoose");
const { v4: uuidv4 } = require("uuid");

const locationSchema = new dynamoose.Schema({
  id: {
    type: String,
    hashKey: true,
    default: uuidv4,
  },
  name: {
    type: String,
    required: true,
  },
  geolocation: {
    type: Array,
    schema: [Number],
    validate: (val) => val.length === 2
  }
});

Location = dynamoose.model("Locations", locationSchema);

module.exports = Location;