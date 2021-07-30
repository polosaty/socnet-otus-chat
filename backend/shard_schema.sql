SET NAMES utf8;
SET time_zone = '+00:00';
SET foreign_key_checks = 0;

-- DROP TABLE IF EXISTS `chat_message`;
CREATE TABLE IF NOT EXISTS `chat_message` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `shard_id` int(11) NOT NULL,
  `chat_id` bigint(20) NOT NULL,
  `timestamp` timestamp NOT NULL,
  `author_id` bigint(20) NOT NULL,
  `content` text COLLATE utf8_unicode_ci NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;


-- DROP TABLE IF EXISTS `read_message`;
CREATE TABLE IF NOT EXISTS `read_message` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `chat_id` bigint(20) NOT NULL,
  `user_id` bigint(20) NOT NULL,
  `message_id` bigint(20) NOT NULL,
  `timespamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `message_id_chat_id_user_id` (`message_id`,`chat_id`,`user_id`),
  CONSTRAINT `read_message_ibfk_1` FOREIGN KEY (`message_id`) REFERENCES `chat_message` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
