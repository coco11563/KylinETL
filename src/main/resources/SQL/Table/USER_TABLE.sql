/*
Navicat MySQL Data Transfer

Source Server         : thiehost
Source Server Version : 50715
Source Host           : localhost:3306
Source Database       : hivedb

Target Server Type    : MYSQL
Target Server Version : 50715
File Encoding         : 65001

Date: 2017-04-12 19:31:03
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for user_table
-- ----------------------------
DROP TABLE IF EXISTS `user_table`;
CREATE TABLE `user_table` (
  `user_id` varchar(255) NOT NULL,
  `gender` varchar(255) NOT NULL,
  PRIMARY KEY (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
