import { DynamoDBClient, ScanCommand } from '@aws-sdk/client-dynamodb';
import { AWS_REGION, AWS_ACCESS_KEY, AWS_SECRET_KEY } from './config';

const client = new DynamoDBClient({
  region: AWS_REGION,
  credentials: {
    accessKeyId: AWS_ACCESS_KEY,
    secretAccessKey: AWS_SECRET_KEY
  }
});

const listTableItems = async (tableName) => {
  try {
    const command = new ScanCommand({ TableName: tableName });
    const response = await client.send(command);
    console.log(`Elementos en la tabla ${tableName}:`);
    console.log(response.Items);
  } catch (e) {
    console.error(`Error al escanear la tabla ${tableName}:`);
    console.error(e);
  }
};

listTableItems('Articles');