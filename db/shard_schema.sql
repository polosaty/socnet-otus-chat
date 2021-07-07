SET NAMES utf8;
SET time_zone = '+00:00';
SET foreign_key_checks = 0;

-- DROP TABLE IF EXISTS `chat_message`;
CREATE TABLE `chat_message` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `shard_id` int(11) NOT NULL,
  `chat_id` bigint(20) NOT NULL,
  `timestamp` timestamp NOT NULL,
  `author_id` bigint(20) NOT NULL,
  `content` text COLLATE utf8_unicode_ci NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
