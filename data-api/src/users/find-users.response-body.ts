import { ApiProperty } from "@nestjs/swagger"

export class FindUsersResponseBody {
  @ApiProperty({
    name: "users",
    description: "A list of users that match the specified filter.",
    nullable: true,
    type: "array",
    items: { type: "CreateUserResponseDto" },
    example: [
      {
        username: "master-api-key",
        type: "api-key",
        role: "admin",
        masked_key: "b8.......0a8d",
        created_at: "2024-08-09T04:30:29.606Z",
        expires_at: "9999-12-31T00:00:00.000Z",
        id: "66b59b6510eadcafd1b17796",
      },
      {
        username: "read-api-key",
        type: "api-key",
        role: "read",
        masked_key: "a7.......7c9d",
        created_at: "2024-08-09T04:30:46.616Z",
        expires_at: "2025-08-09T04:22:31.082Z",
        id: "66b59b7610eadcafd1b17797",
      },
    ],
  })
  documents: Record<string, any>[] | null
}
