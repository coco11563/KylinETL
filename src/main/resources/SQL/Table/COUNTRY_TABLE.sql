/*
Navicat MySQL Data Transfer

Source Server         : thiehost
Source Server Version : 50715
Source Host           : localhost:3306
Source Database       : hivedb

Target Server Type    : MYSQL
Target Server Version : 50715
File Encoding         : 65001

Date: 2017-04-12 19:30:43
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for country_table
-- ----------------------------
DROP TABLE IF EXISTS `country_table`;
CREATE TABLE `country_table` (
  `country_id` varchar(255) NOT NULL,
  `country_name` varchar(255) NOT NULL,
  PRIMARY KEY (`country_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of country_table
-- ----------------------------
