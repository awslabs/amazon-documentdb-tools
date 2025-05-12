import { EJSON } from "bson"
import { Inject, Injectable } from "@nestjs/common"
import { Logger } from "@nestjs/common"
import { InsertOneRequestBody } from "./dto/insert-one-request-body"
import { UpdateRequestBody } from "./dto/update-request-body"
import { InsertOneResponseBody } from "./dto/insert-one-response-body"
import { ErrorResponse } from "./dto/error-response"
import { InsertManyRequestBody } from "./dto/insert-many-request-body"
import { InsertManyResponseBody } from "./dto/insert-many-response-body"
import { FindOneRequestBody } from "./dto/fine-one-request-body"
import { FindOneResponseBody } from "./dto/find-one-response-body"
import { FindRequestBody } from "./dto/fine-request-body"
import { FindResponseBody } from "./dto/find-response-body"
import { UpdateResponseBody } from "./dto/update-response-body"
import { DeleteResponseBody } from "./dto/delete-response-body"
import { AggregateRequestBody } from "./dto/aggregate-request-body"
import { AggregateResponseBody } from "./dto/aggregate-response-body"
import { DeleteRequestBody } from "./dto/delete-request-body"
import { MongoDBProvider } from "../utils/mongodb-provider"

const logger = new Logger("actions.service")

@Injectable()
export class ActionService {
  mongodbProvider: MongoDBProvider
  constructor(@Inject() mongodbProvider: MongoDBProvider) {
    this.mongodbProvider = mongodbProvider
  }

  async insertOne(
    insertOneRequest: InsertOneRequestBody,
  ): Promise<InsertOneResponseBody | ErrorResponse> {
    try {
      let collection = this.mongodbProvider.mongoClient
        .db(insertOneRequest.database)
        .collection(insertOneRequest.collection)
      let result = await collection.insertOne(insertOneRequest.document)
      return {
        insertedId: result.insertedId.toString(),
      }
    } catch (error) {
      return {
        error: error.message,
        error_code: error.code,
        link: "error.link",
      }
    }
  }

  async insertMany(
    insertManyRequest: InsertManyRequestBody,
  ): Promise<InsertManyResponseBody | ErrorResponse> {
    try {
      let collection = this.mongodbProvider.mongoClient
        .db(insertManyRequest.database)
        .collection(insertManyRequest.collection)
      let result = await collection.insertMany(insertManyRequest.documents)
      return {
        insertedCount: result.insertedCount,
        insertedIds: Object.values(result.insertedIds).map((id) =>
          id.toString(),
        ),
      }
    } catch (error) {
      return {
        error: error.message,
        error_code: error.code,
        link: "error.link",
      }
    }
  }

  async findOne(
    findOneRequest: FindOneRequestBody,
  ): Promise<FindOneResponseBody | ErrorResponse> {
    try {
      let collection = this.mongodbProvider.mongoClient
        .db(findOneRequest.database)
        .collection(findOneRequest.collection)
      let result = await collection.findOne(findOneRequest.filter)
      return {
        document: result,
      }
    } catch (error) {
      return {
        error: error.message,
        error_code: error.code,
        link: "error.link",
      }
    }
  }

  async find(
    findRequest: FindRequestBody,
  ): Promise<FindResponseBody | ErrorResponse> {
    try {
      let collection = this.mongodbProvider.mongoClient
        .db(findRequest.database)
        .collection(findRequest.collection)
      let cursor = collection.find(findRequest.filter)
      if (findRequest.sort) cursor = cursor.sort(findRequest.sort)
      if (findRequest.skip) cursor = cursor.skip(findRequest.skip)
      if (findRequest.limit) cursor = cursor.limit(findRequest.limit)
      let result = await cursor.toArray()
      return {
        documents: result,
      }
    } catch (error) {
      return {
        error: error.message,
        error_code: error.code,
        link: "error.link",
      }
    }
  }

  async update(
    updateRequest: UpdateRequestBody,
  ): Promise<UpdateResponseBody | ErrorResponse> {
    try {
      let collection = this.mongodbProvider.mongoClient
        .db(updateRequest.database)
        .collection(updateRequest.collection)
      let result = await collection.updateMany(
        updateRequest.filter,
        updateRequest.update,
      )
      let upsertedId: string | null = result.upsertedId
        ? result.upsertedId.toString()
        : null
      return {
        matchedCount: result.matchedCount,
        modifiedCount: result.modifiedCount,
        upsertedCount: result.upsertedCount,
        upsertedId: upsertedId,
      }
    } catch (error) {
      return {
        error: error.message,
        error_code: error.code,
        link: "error.link",
      }
    }
  }

  async updateOne(
    updateRequest: UpdateRequestBody,
  ): Promise<UpdateResponseBody | ErrorResponse> {
    try {
      let collection = this.mongodbProvider.mongoClient
        .db(updateRequest.database)
        .collection(updateRequest.collection)
      let result = await collection.updateOne(
        updateRequest.filter,
        updateRequest.update,
      )
      let upsertedId: string | null = result.upsertedId
        ? result.upsertedId.toString()
        : null
      return {
        matchedCount: result?.matchedCount,
        modifiedCount: result?.modifiedCount,
        upsertedCount: result?.upsertedCount,
        upsertedId: upsertedId,
      }
    } catch (error) {
      return {
        error: error.message,
        error_code: error.code,
        link: "error.link",
      }
    }
  }

  async deleteOne(
    deleteRequest: DeleteRequestBody,
  ): Promise<DeleteResponseBody | ErrorResponse> {
    try {
      let collection = this.mongodbProvider.mongoClient
        .db(deleteRequest.database)
        .collection(deleteRequest.collection)
      let result = await collection.deleteOne(deleteRequest.filter)
      return {
        deletedCount: result.deletedCount,
      }
    } catch (error) {
      return {
        error: error.message,
        error_code: error.code,
        link: "error.link",
      }
    }
  }

  async delete(
    deleteRequest: DeleteRequestBody,
  ): Promise<DeleteResponseBody | ErrorResponse> {
    try {
      let collection = this.mongodbProvider.mongoClient
        .db(deleteRequest.database)
        .collection(deleteRequest.collection)
      let result = await collection.deleteMany(deleteRequest.filter)
      return {
        deletedCount: result.deletedCount,
      }
    } catch (error) {
      return {
        error: error.message,
        error_code: error.code,
        link: "error.link",
      }
    }
  }

  async aggregate(
    aggregateRequest: AggregateRequestBody,
  ): Promise<AggregateResponseBody | ErrorResponse> {
    try {
      let collection = this.mongodbProvider.mongoClient
        .db(aggregateRequest.database)
        .collection(aggregateRequest.collection)
      let cursor = collection.aggregate(aggregateRequest.pipeline)
      let result = await cursor.toArray()
      return {
        documents: result,
      }
    } catch (error) {
      return {
        error: error.message,
        error_code: error.code,
        link: "error.link",
      }
    }
  }
}
