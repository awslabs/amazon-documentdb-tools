import { ApiProperty } from '@nestjs/swagger';

export class BaseRequestBody {
  @ApiProperty({
    name: "dataSource",
    type: "string",
    required: true,
    description: "The name of a linked Amazon DocumentDB data source.",
    example: "main"
  })
  dataSource: string;

  @ApiProperty({
    name: "database",
    type: "string",
    required: true,
    description: "The name of a database in the specified data source.",
    example: "learn-data-api"
  })
  database: string;


  @ApiProperty({
    name: "collection",
    type: "string",
    required: true,
    description: "The name of a collection in the specified database.",
    example: "tasks"
  })
  collection: string;
}