import { ApiProperty } from '@nestjs/swagger';

export class DeleteResponseBody {

  @ApiProperty({
    name: "deletedCount",
    type: "number",
    required: true,
    description: "The number of documents that were deleted.",
    example: { "deletedCount": 1 }
  })
  deletedCount: number;
}
