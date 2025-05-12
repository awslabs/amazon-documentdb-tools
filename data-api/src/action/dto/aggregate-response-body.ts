import { ApiProperty } from '@nestjs/swagger';

export class AggregateResponseBody {
  @ApiProperty({
    name: "documents",
    type: "array",
    items: { "type": "object" },
    nullable: true,
    description: "A list of documents that match the specified aggregation pipeline.",
    example: [
      {"_id": "completed", "count": 4},
      {"_id": "pending", "count": 6},
    ]
  })
  documents: Record<string, any>[];
}
