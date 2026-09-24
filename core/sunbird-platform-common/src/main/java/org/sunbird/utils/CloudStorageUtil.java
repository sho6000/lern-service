package org.sunbird.utils;

import static org.sunbird.keys.JsonKey.CLOUD_STORAGE_CNAME_URL;
import static org.sunbird.keys.JsonKey.CLOUD_STORE_BASE_PATH;
import static org.sunbird.common.ProjectUtil.getConfigValue;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.sunbird.cloud.storage.IStorageService;
import org.sunbird.cloud.storage.StorageConfig;
import org.sunbird.cloud.storage.StorageServiceFactory;
import org.sunbird.keys.JsonKey;
import org.sunbird.common.ProjectUtil;
import org.sunbird.common.PropertiesCache;

public class CloudStorageUtil {
  private static final int STORAGE_SERVICE_API_RETRY_COUNT = 3;
  private static final Map<String, IStorageService> storageServiceMap = new HashMap<>();

  /**
   * Uploads a file to the cloud storage.
   *
   * @param storageType The type of storage (e.g., azure, aws).
   * @param container The container or bucket name.
   * @param objectKey The key (path) for the object in storage.
   * @param filePath The local path of the file to upload.
   * @return The URL of the uploaded file.
   */
  public static String upload(
      String storageType, String container, String objectKey, String filePath) {
    IStorageService storageService = getStorageService(storageType);
    return storageService.upload(
        container,
        filePath,
        objectKey,
        false,
        1,
        STORAGE_SERVICE_API_RETRY_COUNT,
        null);
  }

  /**
   * Generates a signed URL for an object in cloud storage.
   *
   * @param storageType The type of storage.
   * @param container The container or bucket name.
   * @param objectKey The key of the object.
   * @return The signed URL.
   */
  public static String getSignedUrl(String storageType, String container, String objectKey) {
    IStorageService storageService = getStorageService(storageType);
    return getSignedUrl(storageService, container, objectKey, storageType);
  }

  /**
   * Generates a signed URL for an object using a specific storage service instance.
   *
   * @param storageService The storage service instance.
   * @param container The container or bucket name.
   * @param objectKey The key of the object.
   * @param cloudType The cloud type (not directly used but part of signature).
   * @return The signed URL.
   */
  public static String getSignedUrl(
      IStorageService storageService, String container, String objectKey, String cloudType) {
    return storageService.getSignedURLV2(
        container,
        objectKey,
        getTimeoutInSeconds(),
        "r",
        "application/pdf",
        null);
  }

  /**
   * Deletes a file from cloud storage.
   *
   * @param storageType The type of storage.
   * @param container The container or bucket name.
   * @param objectKey The key of the object to delete.
   */
  public static void deleteFile(String storageType, String container, String objectKey) {
    IStorageService storageService = getStorageService(storageType);
    storageService.deleteObject(container, objectKey, false);
  }

  /**
   * Gets the URI for a specific prefix in the container.
   *
   * @param storageType The type of storage.
   * @param container The container name.
   * @param prefix The prefix to list/get.
   * @param isDirectory Whether it is a directory.
   * @return The URI.
   */
  public static String getUri(
      String storageType, String container, String prefix, boolean isDirectory) {
    IStorageService storageService = getStorageService(storageType);
    return storageService.getUri(container, prefix, isDirectory);
  }

  /**
   * Gets the base URL for cloud storage from configuration.
   *
   * @return The base URL.
   */
  public static String getBaseUrl() {
    String baseUrl = getConfigValue(CLOUD_STORAGE_CNAME_URL);
    if (StringUtils.isEmpty(baseUrl)) baseUrl = getConfigValue(CLOUD_STORE_BASE_PATH);
    return baseUrl;
  }

  /**
   * Retrieves a storage service instance based on the provided storage type.
   * Loads account name and key from properties cache.
   *
   * @param storageType The type of storage (e.g., azure, aws).
   * @return An IStorageService instance.
   */
  private static IStorageService getStorageService(String storageType) {
    String storageKey = PropertiesCache.getInstance().getProperty(JsonKey.ACCOUNT_NAME);
    String storageSecret = PropertiesCache.getInstance().getProperty(JsonKey.ACCOUNT_KEY);
    return getStorageService(storageType, storageKey, storageSecret);
  }

  /**
   * Retrieves or creates a storage service instance.
   * Uses a composite key (type-key) to cache instances.
   *
   * @param storageType The type of storage.
   * @param storageKey The storage account key/name.
   * @param storageSecret The storage account secret.
   * @return An IStorageService instance.
   */
  private static IStorageService getStorageService(
      String storageType, String storageKey, String storageSecret) {
    String compositeKey = storageType + "-" + storageKey;
    if (storageServiceMap.containsKey(compositeKey)) {
      return storageServiceMap.get(compositeKey);
    }
    synchronized (CloudStorageUtil.class) {
      if (storageServiceMap.containsKey(compositeKey)) {
        return storageServiceMap.get(compositeKey);
      }
      String authTypeStr = PropertiesCache.getInstance().getProperty(JsonKey.AUTH_TYPE);
      if (StringUtils.isBlank(authTypeStr)) {
        authTypeStr = "ACCESS_KEY";
      }

      // Resolve auth type from config value (case-insensitive enum name)
      StorageConfig.AuthType authType;
      try {
        authType = StorageConfig.AuthType.valueOf(authTypeStr.replace("-", "_").toUpperCase());
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("Invalid auth type: " + authTypeStr + ". Must be a valid StorageConfig.AuthType enum value.", e);
      }

      // Resolve storage type from config value (case-insensitive enum name)
      StorageConfig.StorageType resolvedStorageType;
      try {
        resolvedStorageType = StorageConfig.StorageType.valueOf(storageType.toUpperCase());
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("Invalid storage type: " + storageType + ". Must be a valid StorageConfig.StorageType enum value.", e);
      }

      StorageConfig.Builder builder = StorageConfig.builder(resolvedStorageType)
              .storageKey(storageKey)
              .authType(authType);

      // For ACCESS_KEY auth, provide the account secret.
      // For OIDC / IAM / IAM_ROLE / INSTANCE_PROFILE, the cloud SDK resolves
      // credentials automatically from Workload Identity env vars or instance
      // metadata — passing a secret would break the credential chain.
      if (authType == StorageConfig.AuthType.ACCESS_KEY) {
        builder.storageSecret(storageSecret);
      }
      // region for providers that need it (e.g. AWS S3 outside us-east-1); no-op when unset
      String region = ProjectUtil.getConfigValue(JsonKey.CLOUD_STORAGE_REGION);
      if (StringUtils.isNotBlank(region)) {
        builder.region(region);
      }
      StorageConfig storageConfig = builder.build();
      IStorageService storageService = StorageServiceFactory.getStorageService(storageConfig);
      storageServiceMap.put(compositeKey, storageService);
    }
    return storageServiceMap.get(compositeKey);
  }

  /**
   * Retrieves the download link expiry timeout from configuration.
   *
   * @return The timeout in seconds.
   */
  private static int getTimeoutInSeconds() {
    String timeoutInSecondsStr = ProjectUtil.getConfigValue(JsonKey.DOWNLOAD_LINK_EXPIRY_TIMEOUT);
    return Integer.parseInt(timeoutInSecondsStr);
  }
}
