const { DynamoDBClient, PutItemCommand, UpdateItemCommand, GetItemCommand } = require('@aws-sdk/client-dynamodb');
const fs = require('fs');
const csv = require('csv-parser');
const config = require('../config');
const path = require('path');

const client = new DynamoDBClient({
  region: config.AWS_REGION,
  credentials: {
    accessKeyId: config.AWS_ACCESS_KEY,
    secretAccessKey: config.AWS_SECRET_KEY
  }
});

const generateId = () => Math.floor(Math.random() * 1000000).toString();

const tagMapping = {
  'Basureros': '1',
  'Río': '2',
  'Agua': '2',
  'Plantas de tratamiento': '3'
};

const actors = {};
const locations = {};
let locationCounter = 1;

const uploadCSVToDynamoDB = async (csvFilePath) => {
  const items = [];

  fs.createReadStream(csvFilePath)
    .pipe(csv())
    .on('data', (row) => {
      const cleanedRow = {};
      for (const key in row) {
        const cleanedKey = key.replace(/[^a-zA-Z0-9_]/g, '');
        cleanedRow[cleanedKey] = row[key] || 'N/A'; 
      }
      items.push(cleanedRow);
    })
    .on('end', async () => {
      console.log(`CSV file ${csvFilePath} successfully processed`);
      
      for (const item of items) {
        if (!item.publicationDate || !item.Headline || !item.name || !item.coverageLevel || !item.author || !item.url || !item.tags || !item.location) {
          console.error(`Error: Missing required fields in item: ${JSON.stringify(item)}`);
          continue; 
        }

        const _articleId = generateId();
        const tags = [tagMapping[item.tags] || ''];
        const author = item.author;
        let actorId = actors[author];
        if (!actorId) {
          actorId = generateId(); 
          actors[author] = actorId;
        }
        let locationId;
        if (locations[item.location]) {
          locationId = locations[item.location];
        } else {
          locationId = locationCounter.toString();
          locations[item.location] = locationId;
          locationCounter++;
        }

        const articleIdNum = parseInt(_articleId, 10);
        const locationIdNum = parseInt(locationId, 10);

        if (isNaN(articleIdNum) || isNaN(locationIdNum)) {
          console.error(`Error: Invalid numeric values in item: ${JSON.stringify(item)}`);
          continue; 
        }

        const articleParams = {
          TableName: 'Articles',
          Item: {
            _articleId: { N: articleIdNum.toString() }, 
            tags: { NS: tags.map(String) }, 
            geolocation: { N: locationIdNum.toString() }, 
            actorsMentioned: { NS: [actorId] },
            publicationDate: { S: item.publicationDate },
            source: {
              M: {
                name: { S: item.name },
                headline: { S: item.Headline },
                url: { S: item.url },
                paywall: { BOOL: true },
                coverageLevel: { S: item.coverageLevel },
                author: { S: item.author }
              }
            }
          }
        };

        try {
          const command = new PutItemCommand(articleParams);
          await client.send(command);
          console.log(`Item insertado en Articles: ${JSON.stringify(item)}`);
        } catch (e) {
          console.error(`Error al insertar el item en Articles: ${JSON.stringify(item)}`);
          console.error(e);
          continue; 
        }

        const getActorParams = {
          TableName: 'Actors',
          Key: { _actorId: { N: actorId.toString() } }
        };

        try {
          const getActorCommand = new GetItemCommand(getActorParams);
          const actorData = await client.send(getActorCommand);

          if (!actorData.Item) {
            const actorParams = {
              TableName: 'Actors',
              Item: {
                _actorId: { N: actorId.toString() }, 
                name: { S: author },
                tag: { S: item.tags },
                articleIds: { NS: [articleIdNum.toString()] } 
              }
            };

            const putActorCommand = new PutItemCommand(actorParams);
            await client.send(putActorCommand);
            console.log(`Actor ${actorId} insertado con articleId ${_articleId}`);
          } else {
            const updateActorParams = {
              TableName: 'Actors',
              Key: { _actorId: { N: actorId.toString() } },
              UpdateExpression: 'ADD articleIds :articleId',
              ExpressionAttributeValues: {
                ':articleId': { NS: [articleIdNum.toString()] }
              }
            };

            const updateActorCommand = new UpdateItemCommand(updateActorParams);
            await client.send(updateActorCommand);
            console.log(`Actor ${actorId} actualizado con articleId ${_articleId}`);
          }
        } catch (e) {
          console.error(`Error al insertar/actualizar el actor ${actorId}: ${e}`);
        }

        for (const [tagName, tagId] of Object.entries(tagMapping)) {
          const tagParams = {
            TableName: 'Tags',
            Item: {
              _idTag: { N: tagId },
              name: { S: tagName }
            }
          };

          try {
            const command = new PutItemCommand(tagParams);
            await client.send(command);
            console.log(`Tag ${tagName} (${tagId}) insertado/actualizado`);
          } catch (e) {
            console.error(`Error al insertar/actualizar el tag ${tagName} (${tagId}): ${e}`);
          }
        }

        const getLocationParams = {
          TableName: 'Locations',
          Key: { '_id Location': { N: locationIdNum.toString() } } 
        };

        try {
          const getLocationCommand = new GetItemCommand(getLocationParams);
          const locationData = await client.send(getLocationCommand);

          if (!locationData.Item) {
            const newLocationParams = {
              TableName: 'Locations',
              Item: {
                '_id Location': { N: locationIdNum.toString() }, 
                name: { S: item.location }
              }
            };

            const putLocationCommand = new PutItemCommand(newLocationParams);
            await client.send(putLocationCommand);
            console.log(`Nueva localización insertada: ${locationId}`);
          }

          const locationParams = {
            TableName: 'Locations',
            Key: { '_id Location': { N: locationIdNum.toString() } }, 
            UpdateExpression: 'ADD articleId :articleId',
            ExpressionAttributeValues: {
              ':articleId': { NS: [articleIdNum.toString()] } 
            }
          };

          const updateLocationCommand = new UpdateItemCommand(locationParams);
          await client.send(updateLocationCommand);
          console.log(`Location ${locationId} actualizado con articleId ${_articleId}`);
        } catch (e) {
          if (e.name === 'ResourceNotFoundException') {
            console.error(`Error al actualizar la location ${locationId}: ${e.message}`);
            continue; 
          } else {
            console.error(`Error al actualizar la location ${locationId}: ${e}`);
          }
        }
      }
    });
};

exports.handler = async (event) => {
  const csvFilePath = path.join(__dirname, 'test.csv');
  await uploadCSVToDynamoDB(csvFilePath);
};