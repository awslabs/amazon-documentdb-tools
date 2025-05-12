import { ApiProperty } from '@nestjs/swagger';

export class InsertOneResponseBody {

  @ApiProperty({
    name: "insertedId",
    type: "string",
    required: true,
    description: "The _id value of the inserted document.",
    example: "667348518a3c6a680157196b"
  })
  insertedId: string;
}
