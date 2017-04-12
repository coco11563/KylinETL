/*
Navicat MySQL Data Transfer

Source Server         : thiehost
Source Server Version : 50715
Source Host           : localhost:3306
Source Database       : hivedb

Target Server Type    : MYSQL
Target Server Version : 50715
File Encoding         : 65001

Date: 2017-04-12 19:30:50
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for province_table
-- ----------------------------
DROP TABLE IF EXISTS `province_table`;
CREATE TABLE `province_table` (
  `province_id` varchar(10) NOT NULL,
  `province_name` varchar(255) NOT NULL,
  PRIMARY KEY (`province_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
