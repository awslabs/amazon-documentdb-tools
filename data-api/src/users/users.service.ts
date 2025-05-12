import { Inject, Injectable } from "@nestjs/common"
import { CreateUserDto } from "./dto/create-user.dto"
import { CreateUserResponseDto } from "./dto/create-user.response.dto"
import { MongoDBProvider } from "../utils/mongodb-provider"
import { createHash } from "crypto"

@Injectable()
export class UsersService {
  mongodbProvider: MongoDBProvider
  constructor(@Inject(MongoDBProvider) mongodbProvider: MongoDBProvider) {
    this.mongodbProvider = mongodbProvider
  }

  async create(createUserDto: CreateUserDto): Promise<CreateUserResponseDto> {
    let doc = this.getDocumentToInsert(createUserDto)
    let result = await this.mongodbProvider.mongoClient
      .db("aws_apps")
      .collection("users")
      .insertOne(doc)
    return this.convertToCreateUserResponseDto(
      doc,
      result.insertedId.toString(),
    )
  }

  async findAll() {
    let result = await this.mongodbProvider.mongoClient
      .db("aws_apps")
      .collection("users")
      .find()
      .toArray()
    return result.map((doc) =>
      this.convertToCreateUserResponseDto(doc, doc._id.toString()),
    )
  }

  convertToCreateUserResponseDto(doc: any, id: string): CreateUserResponseDto {
    let dto = new CreateUserResponseDto()
    dto.username = doc.username
    dto.type = doc.type
    dto.role = doc.role
    dto.key = doc.key
    dto.masked_key = doc.masked_key
    dto.created_at = doc.created_at
    dto.expires_at = doc.expires_at
    dto.id = id
    return dto
  }

  getDocumentToInsert(createUserDto: CreateUserDto) {
    const apiKey = createUserDto.key
    const hashedKey = createHash("sha256").update(apiKey).digest("hex")
    const maskedKey =
      apiKey.substring(0, 2) + "......." + apiKey.substring(apiKey.length - 4)
    return {
      username: createUserDto.username,
      type: createUserDto.type,
      role: createUserDto.role,
      key: createUserDto.key, // well do not store the plain text key
      hashed_key: hashedKey,
      masked_key: maskedKey,
      created_at: new Date(),
      expires_at: createUserDto.expires_at,
    }
  }

  // findAll() {
  //   return `This action returns all users`;
  // }
  //
  // findOne(id: number) {
  //   return `This action returns a #${id} user`;
  // }
  //
  // update(id: number, updateUserDto: UpdateUserDto) {
  //   return `This action updates a #${id} user`;
  // }
  //
  // remove(id: number) {
  //   return `This action removes a #${id} user`;
  // }
}
