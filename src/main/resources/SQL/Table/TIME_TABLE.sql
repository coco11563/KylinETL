/*
Navicat MySQL Data Transfer

Source Server         : thiehost
Source Server Version : 50715
Source Host           : localhost:3306
Source Database       : hivedb

Target Server Type    : MYSQL
Target Server Version : 50715
File Encoding         : 65001

Date: 2017-04-12 19:30:57
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for time_table
-- ----------------------------
DROP TABLE IF EXISTS `time_table`;
CREATE TABLE `time_table` (
  `time_id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `year` varchar(255) NOT NULL,
  `month` varchar(255) NOT NULL,
  `day` varchar(255) NOT NULL,
  `date` datetime NOT NULL ON UPDATE CURRENT_TIMESTAMP,
  `is_holiday` varchar(2) NOT NULL,
  PRIMARY KEY (`time_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
