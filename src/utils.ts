import config from "./config";
import { FastifyBaseLogger } from "fastify";
import fs from "fs";
import { createReadStream } from 'fs';
import fsPromises from "fs/promises";
import { Readable } from "stream";
import { pipeline } from 'stream/promises';
import path from "path";
import { randomUUID } from "crypto";
import { ZodObject, ZodRawShape, ZodTypeAny, ZodDefault } from "zod";
import sharp from "sharp";
import { OutputConversionOptions, WebhookHandlers } from "./types";
import { fetch, RequestInit, Response } from "undici";
// import ObsClient from "esdk-obs-nodejs";


// // 类型定义
// interface UploadResult {
//     ETag?: string;
//     Location: string;
//     RequestId: string;
//   }
  
//   // 初始化客户端
//   const client = new ObsClient({
//     access_key_id: config.HUAWEI_OBS_ACCESS_KEY_ID,
//     secret_access_key: config.HUAWEI_OBS_SECRET_ACCESS_KEY,
//     server: config.HUAWEI_OBS_SERVER,
//   });
  
//   // 通用上传方法
//   export async function uploadFile(
//     objectKey: string,
//     filePath: string
//   ): Promise<UploadResult> {
//     try {
//       const result = await client.putObject({
//         Bucket: config.HUAWEI_OBS_BUCKET,
//         Key: objectKey,
//         Body: createReadStream(filePath)
//       });
  
//       console.log(`[Success] 文件已上传至 OBS: ${result.Location}`);
//       return {
//         ETag: result.ETag,
//         Location: result.Location,
//         RequestId: result.RequestId
//       };
//     } catch (error) {
//       console.error('[Error] 上传失败:', (error as Error).message);
//       throw new Error(`OBS 上传失败: ${(error as Error).message}`);
//     }
//   }
  
export async function sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
}
export async function downloadFile(imageUrl: string, outputPath: string, log: FastifyBaseLogger) {
    // 添加 AbortController 实现超时控制
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 30_000); // 30秒超时

    try {
        const response = await fetch(imageUrl, {
            signal: controller.signal
        });

        if (!response.ok) {
            throw new Error(`下载失败: ${response.status} ${response.statusText}`);
        }

        const body = response.body;
        if (!body) {
            throw new Error("响应体为空");
        }

        // 严格类型校验
        if (!(body instanceof ReadableStream)) {
            throw new Error("无效的流类型");
        }

        const nodeReadable = Readable.fromWeb(body as import('stream/web').ReadableStream);
        const fileStream = fs.createWriteStream(outputPath);

        // 使用 pipeline 替代 pipe
        await pipeline(
            nodeReadable,
            fileStream,
            { signal: controller.signal }
        );

        log.info(`file downloaded and saved to ${outputPath}`);
    } catch (err) {
        // 清理未完成的文件
        if (fs.existsSync(outputPath)) {
            fs.unlinkSync(outputPath);
        }
        throw new Error(`下载失败: ${err instanceof Error ? err.message : err}`);
    } finally {
        clearTimeout(timeout);
    }
}

export async function processImage(
    imageInput: string,
    log: FastifyBaseLogger,
    dirWithinInputDir?: string
): Promise<string> {
    let localFilePath: string;
    const ext = path.extname(imageInput).split("?")[0];
    const localFileName = `${randomUUID()}${ext}`;
    if (dirWithinInputDir) {
        localFilePath = path.join(
            config.inputDir,
            dirWithinInputDir,
            localFileName
        );
        // Create the directory if it doesn't exist
        await fsPromises.mkdir(path.dirname(localFilePath), { recursive: true });
    } else {
        localFilePath = path.join(config.inputDir, localFileName);
    }
    // If image is a url, download it
    if (imageInput.startsWith("http")) {
        await downloadFile(imageInput, localFilePath, log);
        return localFilePath;
    }
    // If image is already a local path, return it as an absolute path
    else if (
        imageInput.startsWith("/") ||
        imageInput.startsWith("./") ||
        imageInput.startsWith("../")
    ) {
        return path.resolve(imageInput);
    } else {
        // Assume it's a base64 encoded image
        try {
            const base64Data = Buffer.from(imageInput, "base64");
            const image = sharp(base64Data);
            const metadata = await image.metadata();
            if (!metadata.format) {
                throw new Error("Failed to parse image metadata");
            }
            localFilePath = `${localFilePath}.${metadata.format}`;
            log.debug(`Saving decoded image to ${localFilePath}`);
            await fsPromises.writeFile(localFilePath, base64Data);
            return localFilePath;
        } catch (e: any) {
            throw new Error(`Failed to parse base64 encoded image: ${e.message}`);
        }
    }
}

export function zodToMarkdownTable(schema: ZodObject<ZodRawShape>): string {
    const shape = schema.shape;
    let markdownTable = "| Field | Type | Description | Default |\n|-|-|-|-|\n";

    for (const [key, value] of Object.entries(shape)) {
        const fieldName = key;
        const { type: fieldType, isOptional } = getZodTypeName(value);
        const fieldDescription = getZodDescription(value);
        const defaultValue = getZodDefault(value);

        markdownTable += `| ${fieldName} | ${fieldType}${isOptional ? "" : ""
            } | ${fieldDescription} | ${defaultValue || "**Required**"} |\n`;
    }

    return markdownTable;
}

function getZodTypeName(zodType: ZodTypeAny): {
    type: string;
    isOptional: boolean;
} {
    let currentType = zodType;
    let isOptional = false;

    while (currentType instanceof ZodDefault) {
        currentType = currentType._def.innerType;
    }

    if (currentType._def.typeName === "ZodOptional") {
        isOptional = true;
        currentType = currentType._def.innerType;
    }

    let type: string;
    switch (currentType._def.typeName) {
        case "ZodString":
            type = "string";
            break;
        case "ZodNumber":
            type = "number";
            break;
        case "ZodBoolean":
            type = "boolean";
            break;
        case "ZodArray":
            type = `${getZodTypeName(currentType._def.type).type}[]`;
            break;
        case "ZodObject":
            type = "object";
            break;
        case "ZodEnum":
            type = `enum (${(currentType._def.values as string[])
                .map((val: string) => `\`${val}\``)
                .join(", ")})`;
            break;
        case "ZodUnion":
            type = currentType._def.options
                .map((opt: any) => getZodTypeName(opt).type)
                .join(", ");
            break;
        case "ZodLiteral":
            type = `literal (${JSON.stringify(currentType._def.value)})`;
            break;
        default:
            type = currentType._def.typeName.replace("Zod", "").toLowerCase();
    }

    return { type, isOptional };
}

function getZodDescription(zodType: ZodTypeAny): string {
    let currentType: ZodTypeAny | undefined = zodType;
    while (currentType) {
        if (currentType.description) {
            return currentType.description;
        }
        currentType = currentType._def.innerType;
    }
    return "";
}

function getZodDefault(zodType: ZodTypeAny): string {
    if (zodType instanceof ZodDefault) {
        const defaultValue = zodType._def.defaultValue();
        return JSON.stringify(defaultValue);
    }
    return "-";
}

export async function convertImageBuffer(
    imageBuffer: Buffer,
    options: OutputConversionOptions
) {
    const { format, options: conversionOptions } = options;
    let image = sharp(imageBuffer);

    if (format === "webp") {
        image = image.webp(conversionOptions);
    } else {
        image = image.jpeg(conversionOptions);
    }

    return image.toBuffer();
}

export async function sendSystemWebhook(
    eventName: string,
    data: any,
    log: FastifyBaseLogger
): Promise<void> {
    const metadata: Record<string, string> = { ...config.systemMetaData };
    if (config.saladContainerGroupId) {
        metadata["salad_container_group_id"] = config.saladContainerGroupId;
    }
    if (config.saladMachineId) {
        metadata["salad_machine_id"] = config.saladMachineId;
    }
    if (config.systemWebhook) {
        try {
            const response = await fetch(config.systemWebhook, {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                },
                body: JSON.stringify({ event: eventName, data, metadata }),
            });

            if (!response.ok) {
                log.error(`Failed to send system webhook: ${await response.text()}`);
            }
        } catch (error) {
            log.error("Error sending system webhook:", error);
        }
    }
}

/**
 * Converts a snake_case string to UpperCamelCase
 */
function snakeCaseToUpperCamelCase(str: string): string {
    const camel = str.replace(/(_\w)/g, (match) => match[1].toUpperCase());
    const upperCamel = camel.charAt(0).toUpperCase() + camel.slice(1);
    return upperCamel;
}

export function getConfiguredWebhookHandlers(
    log: FastifyBaseLogger
): WebhookHandlers {
    const handlers: Record<string, (d: any) => void> = {};
    if (config.systemWebhook) {
        const systemWebhookEvents = config.systemWebhookEvents;
        for (const eventName of systemWebhookEvents) {
            const handlerName = `on${snakeCaseToUpperCamelCase(eventName)}`;
            handlers[handlerName] = (data: any) => {
                log.debug(`Sending system webhook for event: ${eventName}`);
                sendSystemWebhook(`comfy.${eventName}`, data, log);
            };
        }
    }

    return handlers as WebhookHandlers;
}

export async function fetchWithRetries(
    url: string,
    options: RequestInit,
    maxRetries: number,
    log: FastifyBaseLogger
): Promise<Response> {
    let retries = 0;
    while (retries < maxRetries) {
        try {
            const response = await fetch(url, options);
            if (response.ok) {
                return response;
            }
            log.error(
                `Failed to fetch ${url}: ${response.status} ${response.statusText}`
            );
        } catch (error) {
            log.error(`Error fetching ${url}: ${error}`);
        }
        retries++;
        await sleep(1000);
    }
    throw new Error(`Failed to fetch ${url} after ${maxRetries} retries`);
}
