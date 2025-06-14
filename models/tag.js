const dynamoose = require("dynamoose");
const { v4: uuidv4 } = require("uuid");

const tagSchema = new dynamoose.Schema({
  id: {
    type: String,
    hashKey: true,
    default: uuidv4,
  },
  name: {
    type: String,
    required: true,
  },
});

const Tag = dynamoose.model("Tags", tagSchema);

module.exports = Tag;