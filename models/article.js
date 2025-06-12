const dynamoose = require('dynamoose');
const { v4: uuidv4 } = require('uuid');

const articleSchema = new dynamoose.Schema({
  id: {
    type: String,
    hashKey: true,
    default: uuidv4
  },
  publicationDate: {
    type: String,
    required: true,
  },
  sourceName: {
    type: String,
    required: true,
  },
  paywall: {
    type: Boolean,
  },
  headline: {
    type: String,
    required: true,
  },
  url: {
    type: String,
    required: true,
    index: {
      global: true,
      name: 'url-index',
    },
  },
  author: {
    type: String,
  },
  coverageLevel: {
    type: String,
  },
  actorsMentioned: {
    type: Array,
    schema: [String],
  },
  tags: {
    type: Array,
    schema: [String],
    required: true,
  },
  location: {
    type: String,
    required: true,
  },
})


const Article = dynamoose.model('Articles', articleSchema);

module.exports = Article;
