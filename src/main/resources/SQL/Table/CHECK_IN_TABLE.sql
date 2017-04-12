/*
Navicat MySQL Data Transfer

Source Server         : thiehost
Source Server Version : 50715
Source Host           : localhost:3306
Source Database       : hivedb

Target Server Type    : MYSQL
Target Server Version : 50715
File Encoding         : 65001

Date: 2017-04-12 19:30:25
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for check_in_table
-- ----------------------------
DROP TABLE IF EXISTS `check_in_table`;
CREATE TABLE `check_in_table` (
  `weibo_id` varchar(255) NOT NULL,
  `json_file_content` text NOT NULL,
  `content` varchar(255) NOT NULL,
  PRIMARY KEY (`weibo_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
