const fs = require('fs');
const dynamoose = require('dynamoose')
const csv = require('csv-parser');
const config = require('./config');
const _ = require('lodash');
const Article = require('./models/article');
const Actor = require('./models/actor');
const Location = require('./models/location');
const Tag = require('./models/tag');

const data = [];
const tags = {};
const Locations = {};

const ddb = new dynamoose.aws.ddb.DynamoDB({
  region: config.AWS_REGION,
  credentials: {
    accessKeyId: config.AWS_ACCESS_KEY,
    secretAccessKey: config.AWS_SECRET_KEY
  },
});
dynamoose.aws.ddb.set(ddb);

const csvFilePath = './test_copy.csv';

function cleanString(str) {
  return str
  .normalize('NFD')
  .replace(/[\u0300-\u036f]/g, '')
  .toUpperCase()
  .trim()
  .replace(/\s+/g, '_')
  .replace(/,/g, '');
}

function replaceNamesWithIds(data, tags, locations) {
  const tagNameToId = {};
  tags.forEach(tagObj => {
    tagNameToId[tagObj.name] = tagObj.id;
  });

  const locationNameToId = {};
  locations.forEach(locObj => {
    locationNameToId[locObj.name] = locObj.id;
  });

  data.forEach(el => {
    if (el.tags && tagNameToId[el.tags]) {
      el.tags = tagNameToId[el.tags];
    }
    if (el.location && locationNameToId[el.location]) {
      el.location = locationNameToId[el.location];
    }
  });

  return data;
}

async function uploadTags(tags) {
  const uniqueTags = Object.keys(tags);
  for (const tag of uniqueTags){
    const tagExists = await Tag.scan('name').eq(tag).exec();
    if (!tagExists.length) {
      const newTag = new Tag({
        name: tag
      })
      await newTag.save()
    }
  }
  const currentTags = await Tag.scan().exec();
  return JSON.parse(JSON.stringify(currentTags));
}

async function uploadLocations(locations) {
  const uniqueLocations = Object.keys(locations);
  for (const location of uniqueLocations) {
    const locationExists = await Location.scan('name').eq(location).exec();
    if (!locationExists.length) {
      const newLocation = new Location({
        name: location
      });
      await newLocation.save();
    }
  }
  const currentLocations = await Location.scan().exec();
  return JSON.parse(JSON.stringify(currentLocations));
}

async function uploadActors(data, articles) {
  const articleUrlToId = {};
  articles.forEach(articleObject => {
    articleUrlToId[articleObject.url] = articleObject.id;
  });

  const actors = Object.keys(data);

  for (const actor of actors) {
    const articlesByActor = data[actor];
    const articleIds = articlesByActor.map(article => articleUrlToId[article.url]);
    const actorExists = await Actor.scan('name').eq(actor).exec();
    if (!actorExists.length) {
      const newActor = new Actor({
        name: actor,
        articleIds: articleIds
      });
      await newActor.save();
    } 
    else {
      const existingActor = actorExists[0];
      const updatedArticleIds = Array.from(new Set([...(existingActor.articleIds || []), ...articleIds]));
      await Actor.update({ id: existingActor.id }, { articleIds: updatedArticleIds });
    }
  }
  const currentActors = await Actor.scan().exec();
  return JSON.parse(JSON.stringify(currentActors));
}

async function uploadArticles(data) {
  for (const el of data) {
    const articleExists = await Article.scan('url').eq(el.url).exec();
    if (!articleExists.length) {
      const newArticle = new Article({
        publicationDate: el.publicationDate,
        sourceName: el.name,
        headline: el.Headline,
        url: el.url,
        author: el.author,
        coverageLevel: el.coverageLevel,
        tags: [el.tags],
        location: el.location,
      });
      await newArticle.save()
    }
  }
  const currentArticles = await Article.scan().exec();
  return JSON.parse(JSON.stringify(currentArticles));
}

async function addActorIdToArticles(actorsWithIds) {
  for (const actor of actorsWithIds) {
    for (const articleId of actor.articleIds) {
      const articleArr = await Article.scan('id').eq(articleId).exec();
      const article = articleArr[0];
      let actorsMentioned = Array.isArray(article.actorsMentioned) ? article.actorsMentioned : [];
      if (!actorsMentioned.includes(actor.id)) {
        actorsMentioned.push(actor.id);
        await Article.update({ id: article.id }, { actorsMentioned });
      }
    }
  }
}

fs.createReadStream(csvFilePath)
  .pipe(csv())
  .on('data', (row) => {
    if(row.tags) {
      const cleanedTag = cleanString(row.tags);
      if(!tags[cleanedTag]) {
        tags[cleanedTag] = cleanedTag;
      }
      row.tags = cleanedTag;
    }
    if(row.location) {
      const cleanedLocation = cleanString(row.location);
      if(!Locations[cleanedLocation]) {
        Locations[cleanedLocation] = cleanedLocation;
      }
      row.location = cleanedLocation;
    }

    data.push(row);
  })
  .on('end', () => {
    (async () => {
      const currentTags = await uploadTags(tags);
      const currentLocations = await uploadLocations(Locations);
      const cleanedData = replaceNamesWithIds(data, currentTags, currentLocations);
      const currentArticles = await uploadArticles(cleanedData);
      const groupedData = _.groupBy(data, 'actor');
      const currentActors = await uploadActors(groupedData, currentArticles);
      await addActorIdToArticles(currentActors);
    })();
  });