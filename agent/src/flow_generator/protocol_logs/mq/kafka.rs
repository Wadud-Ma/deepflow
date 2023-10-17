/*
 * Copyright (c) 2023 Yunshan Networks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use serde::Serialize;

use crate::{
    common::{
        enums::IpProtocol,
        flow::{L7PerfStats, L7Protocol, PacketDirection},
        l7_protocol_info::{L7ProtocolInfo, L7ProtocolInfoInterface},
        l7_protocol_log::{KafkaInfoCache, L7ParseResult, L7ProtocolParserInterface, ParseParam},
    },
    flow_generator::{
        error::{Error, Result},
        protocol_logs::{
            consts::KAFKA_REQ_HEADER_LEN,
            pb_adapter::{ExtendedInfo, L7ProtocolSendLog, L7Request, L7Response},
            value_is_default, value_is_negative, AppProtoHead, L7ResponseStatus, LogMessageType,
        },
    },
    utils::bytes::{read_i16_be, read_u16_be, read_u32_be, read_u8_be},
};
use log::info;

const KAFKA_FETCH: u16 = 1;
const FILTER_TOPIC_ARRAY: [&str; 3] = ["ketrace-test-java-02", "ketrace-php-segment-test", "ketrace-agent-log-test"];

#[derive(Serialize, Debug, Default, Clone)]
pub struct KafkaInfo {
    msg_type: LogMessageType,
    #[serde(skip)]
    is_tls: bool,

    #[serde(rename = "request_id", skip_serializing_if = "value_is_default")]
    pub correlation_id: u32,

    // request
    #[serde(rename = "request_length", skip_serializing_if = "value_is_negative")]
    pub req_msg_size: Option<u32>,
    #[serde(skip)]
    pub api_version: u16,
    #[serde(rename = "request_type")]
    pub api_key: u16,
    #[serde(skip)]
    pub client_id: String,
    #[serde(rename = "request_resource")]
    pub publish_topic: Option<String>,

    // reponse
    #[serde(rename = "response_length", skip_serializing_if = "value_is_negative")]
    pub resp_msg_size: Option<u32>,
    #[serde(rename = "response_status")]
    pub status: L7ResponseStatus,
    #[serde(rename = "response_code", skip_serializing_if = "Option::is_none")]
    pub status_code: Option<i32>,

    rrt: u64,
}

impl L7ProtocolInfoInterface for KafkaInfo {
    fn session_id(&self) -> Option<u32> {
        Some(self.correlation_id)
    }

    fn merge_log(&mut self, other: crate::common::l7_protocol_info::L7ProtocolInfo) -> Result<()> {
        if let L7ProtocolInfo::KafkaInfo(other) = other {
            self.merge(other);
        }
        Ok(())
    }

    fn app_proto_head(&self) -> Option<AppProtoHead> {
        Some(AppProtoHead {
            proto: L7Protocol::Kafka,
            msg_type: self.msg_type,
            rrt: self.rrt,
        })
    }

    fn is_tls(&self) -> bool {
        self.is_tls
    }
}

impl KafkaInfo {
    // https://kafka.apache.org/protocol.html
    const API_KEY_MAX: u16 = 67;
    pub fn merge(&mut self, other: Self) {
        if self.resp_msg_size.is_none() {
            self.resp_msg_size = other.resp_msg_size;
        }
        if other.status != L7ResponseStatus::default() {
            self.status = other.status;
        }
        if other.status_code != None {
            self.status_code = other.status_code;
        }
    }

    pub fn check(&self) -> bool {
        if self.api_key > Self::API_KEY_MAX {
            return false;
        }
        return self.client_id.len() > 0 && self.client_id.is_ascii();
    }

    pub fn get_command(&self) -> &'static str {
        let command_str = [
            "Produce",
            "Fetch",
            "ListOffsets",
            "Metadata",
            "LeaderAndIsr",
            "StopReplica",
            "UpdateMetadata",
            "ControlledShutdown",
            "OffsetCommit",
            "OffsetFetch",
            // 10
            "FindCoordinator",
            "JoinGroup",
            "Heartbeat",
            "LeaveGroup",
            "SyncGroup",
            "DescribeGroups",
            "ListGroups",
            "SaslHandshake",
            "ApiVersions",
            "CreateTopics",
            // 20
            "DeleteTopics",
            "DeleteRecords",
            "InitProducerId",
            "OffsetForLeaderEpoch",
            "AddPartitionsToTxn",
            "AddOffsetsToTxn",
            "EndTxn",
            "WriteTxnMarkers",
            "TxnOffsetCommit",
            "DescribeAcls",
            // 30
            "CreateAcls",
            "DeleteAcls",
            "DescribeConfigs",
            "AlterConfigs",
            "AlterReplicaLogDirs",
            "DescribeLogDirs",
            "SaslAuthenticate",
            "CreatePartitions",
            "CreateDelegationToken",
            "RenewDelegationToken",
            // 40
            "ExpireDelegationToken",
            "DescribeDelegationToken",
            "DeleteGroups",
            "ElectLeaders",
            "IncrementalAlterConfigs",
            "AlterPartitionReassignments",
            "ListPartitionReassignments",
            "OffsetDelete",
            "DescribeClientQuotas",
            "AlterClientQuotas",
            //50
            "DescribeUserScramCredentials",
            "AlterUserScramCredentials",
            "AlterIsr",
            "UpdateFeatures",
            "DescribeCluster",
            "DescribeProducers",
            "DescribeTransactions",
            "ListTransactions",
            "AllocateProducerIds",
        ];
        match self.api_key {
            0..=58 => command_str[self.api_key as usize],
            _ => "",
        }
    }
}

impl From<KafkaInfo> for L7ProtocolSendLog {
    fn from(f: KafkaInfo) -> Self {
        let command_str = f.get_command();
        let log = L7ProtocolSendLog {
            req_len: f.req_msg_size,
            resp_len: f.resp_msg_size,
            req: L7Request {
                req_type: String::from(command_str),
                resource: f.publish_topic.unwrap_or_default(),
                ..Default::default()
            },
            resp: L7Response {
                status: f.status,
                code: f.status_code,
                ..Default::default()
            },
            ext_info: Some(ExtendedInfo {
                request_id: Some(f.correlation_id),
                ..Default::default()
            }),
            ..Default::default()
        };
        return log;
    }
}

#[derive(Clone, Serialize, Default)]
pub struct KafkaLog {
    #[serde(skip)]
    perf_stats: Option<L7PerfStats>,
}

impl L7ProtocolParserInterface for KafkaLog {
    fn check_payload(&mut self, payload: &[u8], param: &ParseParam) -> bool {
        if !param.ebpf_type.is_raw_protocol()
            || param.l4_protocol != IpProtocol::TCP
            || payload.len() < KAFKA_REQ_HEADER_LEN
        {
            return false;
        }
        let mut info = KafkaInfo::default();
        let ok = self.request(payload, true, &mut info, param).is_ok() && info.check();
        self.reset();
        ok
    }

    fn parse_payload(&mut self, payload: &[u8], param: &ParseParam) -> Result<L7ParseResult> {
        if self.perf_stats.is_none() && param.parse_perf {
            self.perf_stats = Some(L7PerfStats::default())
        };
        let mut info = KafkaInfo::default();
        Self::parse(self, payload, param.l4_protocol, param.direction, &mut info, param)?;

        // filter message when the request_type field is empty
        let command_str = info.get_command();
        if command_str.is_empty() {
            return Ok(L7ParseResult::None);
        }

        // filter message whith ketrace topic
        if let Some(search_str) = &info.publish_topic {
            if FILTER_TOPIC_ARRAY.contains(&search_str.as_str()) {
                return Ok(L7ParseResult::None);
            }
        }

        // handle kafka status code
        {
            let mut log_cache = param.l7_perf_cache.borrow_mut();
            if let Some(previous) = log_cache.rrt_cache.get(&info.cal_cache_key(param)) {
                match (previous.msg_type, info.msg_type) {
                    (LogMessageType::Request, LogMessageType::Response)
                        if param.time < previous.time + param.rrt_timeout as u64 =>
                    {
                        if let Some(req) = previous.kafka_info.as_ref() {
                            self.set_status_code(
                                req.api_key,
                                req.api_version,
                                read_i16_be(&payload[12..]),
                                &mut info,
                            )
                        }
                    }
                    (LogMessageType::Response, LogMessageType::Request)
                        if previous.time < param.time + param.rrt_timeout as u64 =>
                    {
                        if let Some(resp) = previous.kafka_info.as_ref() {
                            self.set_status_code(
                                info.api_key,
                                info.api_version,
                                resp.code,
                                &mut info,
                            )
                        }
                    }
                    _ => {}
                }
            }
        }

        info.cal_rrt(
            param,
            Some(KafkaInfoCache {
                api_key: info.api_key,
                api_version: info.api_version,
                code: read_i16_be(&payload[12..]),
            }),
        )
        .map(|rrt| {
            info.rrt = rrt;
            self.perf_stats.as_mut().map(|p| p.update_rrt(rrt));
        });
        if param.parse_log {
            Ok(L7ParseResult::Single(L7ProtocolInfo::KafkaInfo(info)))
        } else {
            Ok(L7ParseResult::None)
        }
    }

    fn protocol(&self) -> L7Protocol {
        L7Protocol::Kafka
    }

    fn parsable_on_udp(&self) -> bool {
        false
    }

    fn perf_stats(&mut self) -> Option<L7PerfStats> {
        self.perf_stats.take()
    }
}

impl KafkaLog {
    const MSG_LEN_SIZE: usize = 4;

    // 协议识别的时候严格检查避免误识别，日志解析的时候不用严格检查因为可能有长度截断
    // ================================================================================
    // The protocol identification is strictly checked to avoid misidentification.
    // The log analysis is not strictly checked because there may be length truncation
    fn request(&mut self, payload: &[u8], strict: bool, info: &mut KafkaInfo, param: &ParseParam) -> Result<()> {
        let req_len = read_u32_be(payload);
        info.req_msg_size = Some(req_len);
        let client_id_len = read_u16_be(&payload[12..]) as usize;
        if payload.len() < KAFKA_REQ_HEADER_LEN + client_id_len {
            return Err(Error::KafkaLogParseFailed);
        }

        if strict && req_len as usize != payload.len() - Self::MSG_LEN_SIZE {
            return Err(Error::KafkaLogParseFailed);
        }

        info.msg_type = LogMessageType::Request;
        info.api_key = read_u16_be(&payload[4..]);
        info.api_version = read_u16_be(&payload[6..]);
        info.correlation_id = read_u32_be(&payload[8..]);
        info.client_id = String::from_utf8_lossy(&payload[14..14 + client_id_len]).into_owned();

        let client_id_end = 14 + client_id_len;
        // parse_payload 解析 topic
        if !strict && payload.len() > KAFKA_REQ_HEADER_LEN + client_id_len {
            self.parse_body(payload, info, client_id_end, param)?;
        }

        if !info.client_id.is_ascii() {
            return Err(Error::KafkaLogParseFailed);
        }
        Ok(())
    }

    // 协议解析，不同api_key 和 api_version 解析方式不同
    // https://kafka.apache.org/protocol.html#protocol_details
    fn parse_body(&mut self, payload: &[u8], info: &mut KafkaInfo, start: usize, param: &ParseParam) -> Result<()> {
        let req_type = info.get_command();
        match req_type {
            "Produce" => {
                self.parse_produce_message(payload, info, start, param)?;
            }
            "Fetch" => {
                self.parse_fetch_message(payload, info, start, param)?;
            }
            "OffsetCommit" => {
                self.parse_offset_commit_message(payload, info, start, param)?;
            }
            _ => {}
        }

        Ok(())
    }

    // 解析 produce request
    fn parse_produce_message(&mut self, payload: &[u8], info: &mut KafkaInfo, start: usize, param: &ParseParam) -> Result<()> {
        let api_version = info.api_version;
        let mut step = 10;
        match api_version {
            0..=2 => {
                let index = start + step;
                self.parse_topic_name(payload, index, info)?;
            }
            3..=9 => {
                step = 12;
                let index = start + step;
                self.parse_topic_name(payload, index, info)?;
            }
            _ => {
                info!("Skip parsing topic metadata in kafka produce request message, current api_version: {:?}, payload: {:?}, param: {:?}", api_version, payload, param)
            }
        }

        Ok(())
    }

    // 解析 fetch request
    fn parse_fetch_message(&mut self, payload: &[u8], info: &mut KafkaInfo, start: usize, param: &ParseParam) -> Result<()> {
        let api_version = info.api_version;
        let mut step = 20;
        match api_version {
            0..=2 => {
                let index = start + step;
                self.parse_topic_name(payload, index, info)?;
            }
            3 => {
                step = 20;
                let index = start + step;
                self.parse_topic_name(payload, index, info)?;
            }
            4..=6 => {
                step = 21;
                let index = start + step;
                self.parse_topic_name(payload, index, info)?;
            }
            7..=15 => {
                step = 29;
                let index = start + step;
                self.parse_topic_name(payload, index, info)?;
            }
            _ => {
                info!("Skip parsing topic metadata in kafka fetch request message， current api_version: {:?}, payload: {:?}, param: {:?}", api_version, payload, param)
            }
        }
        Ok(())
    }

    // 解析 OffsetCommit request
    fn parse_offset_commit_message(&mut self, payload: &[u8], info: &mut KafkaInfo, start: usize, param: &ParseParam) -> Result<()> {
        let api_version = info.api_version;
        // let req_type = info.get_command();
        match api_version {
            1..=7 => {
                if payload.len() > start {
                    let mut s_index = start.clone();
                    // string group_id
                    let group_id_len = read_u16_be(&payload[s_index..]) as usize;
                    s_index += 2;
                    let mut e_index = s_index + group_id_len;
                    // let group_id = String::from_utf8_lossy(&payload[s_index..e_index]).into_owned();

                    // int32 generation_id
                    // let generation_id = read_u32_be(&payload[e_index..]);

                    // string member_id
                    s_index = e_index + 4;
                    if payload.len() < s_index{
                        return Ok(())
                    }
                    let member_id_len = read_u16_be(&payload[s_index..]) as usize;
                    s_index += 2;
                    e_index= s_index + member_id_len;
                    // let member_id = String::from_utf8_lossy(&payload[s_index..e_index]).into_owned();

                    // 为什么+12 而不是+8 呢?
                    if api_version >= 2 && api_version <= 4 {
                        // int64 retention_time_ms
                        // let retention_time_ms = read_u64_be(&payload[e_index..]);
                        s_index = e_index + 12;
                    }else {
                        s_index = e_index + 4;
                    }
                    if payload.len() < s_index{
                        return Ok(())
                    }
                    let topic_len = read_u16_be(&payload[s_index..]) as usize;
                    s_index += 2;
                    e_index = s_index + topic_len;
                    if payload.len() < s_index || payload.len() < e_index {
                        return Ok(())
                    }
                    let topic_name = String::from_utf8_lossy(&payload[s_index..e_index]).into_owned();
                    if !topic_name.is_empty() && topic_name.is_ascii() {
                        info.publish_topic = Some(topic_name);
                        // info!("Kafka Topic name parsed success. current topic_name: {:?}, current api_key: {:?}, current api_version: {:?}, current payload: {:?}", info.publish_topic, req_type, info.api_version, payload);
                    }
                }
            }
            8 => {
                if payload.len() > start {
                    let mut s_index = start.clone() + 1;
                    let group_id_len = read_u8_be(&payload[s_index..]) as usize;
                    s_index += 1;
                    let mut e_index = s_index + group_id_len - 1;
                    // let group_id = String::from_utf8_lossy(&payload[s_index..e_index.clone()]).into_owned();

                    // let generation_id = read_u16_be(&payload[e_index..]);

                    s_index = e_index + 4;
                    if payload.len() < s_index{
                        return Ok(())
                    }
                    let member_id_len = read_u8_be(&payload[s_index..]) as usize ;
                    s_index += 1;
                    e_index= s_index + member_id_len - 1;
                    // let member_id = String::from_utf8_lossy(&payload[s_index..e_index]).into_owned();

                    s_index = e_index + 2;
                    if payload.len() < s_index{
                        return Ok(())
                    }
                    let topic_len = read_u8_be(&payload[s_index..]) as usize;
                    s_index += 1;
                    e_index = s_index + topic_len - 1;
                    if payload.len() < s_index || payload.len() < e_index {
                        return Ok(())
                    }
                    let topic_name = String::from_utf8_lossy(&payload[s_index..e_index]).into_owned();
                    if !topic_name.is_empty() && topic_name.is_ascii() {
                        info.publish_topic = Some(topic_name);
                    }
                }
            }
            _ => {
                info!("Skip parsing topic metadata in kafka OffsetCommit request message， current api_version: {:?}, current payload: {:?}, param: {:?}", api_version, payload, param);
            }
        }
        Ok(())
    }

    fn parse_topic_name(&mut self, payload: &[u8], index: usize, info: &mut KafkaInfo) -> Result<()> {
        if payload.len() > index {
            let body = &payload[index..];
            let topic_len = read_u16_be(&body[..]) as usize;
            let topic_end_index = (2 + topic_len) as usize;
            // let req_type = info.get_command();
            if topic_len > 0 && body.len() > topic_end_index {
                // 前两个字节为长度
                let topic_name_bytes: Vec<u8> = body[2..topic_end_index].to_vec();
                if let Ok(topic_name) = String::from_utf8(topic_name_bytes) {
                    if !topic_name.is_empty() && topic_name.is_ascii() {
                        info.publish_topic = Some(topic_name);
                        // info!("Kafka Topic name parsed success. current topic_name: {:?}, current api_key: {:?}, current api_version: {:?}, current payload: {:?}", info.publish_topic, req_type, info.api_version, payload);
                    } else {
                        info!("Kafka Topic name is not a valid ASCII string or is empty. payload: {:?}", payload);
                    }
                } else {
                    info!("Failed to decode kafka topic name. payload: {:?}", payload);
                }
            }
        }
        Ok(())
    }

    fn response(&mut self, payload: &[u8], info: &mut KafkaInfo) -> Result<()> {
        info.resp_msg_size = Some(read_u32_be(payload));
        info.correlation_id = read_u32_be(&payload[4..]);
        info.msg_type = LogMessageType::Response;
        Ok(())
    }

    fn parse(
        &mut self,
        payload: &[u8],
        proto: IpProtocol,
        direction: PacketDirection,
        info: &mut KafkaInfo,
        param: &ParseParam
    ) -> Result<()> {
        if proto != IpProtocol::TCP {
            return Err(Error::InvalidIpProtocol);
        }
        if payload.len() < KAFKA_REQ_HEADER_LEN {
            return Err(Error::KafkaLogParseFailed);
        }
        match direction {
            PacketDirection::ClientToServer => {
                self.request(payload, false, info, param)?;
                self.perf_stats.as_mut().map(|p| p.inc_req());
            }
            PacketDirection::ServerToClient => {
                self.response(payload, info)?;
                self.perf_stats.as_mut().map(|p| p.inc_resp());
            }
        }
        Ok(())
    }

    /*
        reference:  https://kafka.apache.org/protocol.html#protocol_messages

        only fetch api and api version > 7 parse the error code

        Fetch Response (Version: 7) => throttle_time_ms error_code session_id [responses]
            throttle_time_ms => INT32
            error_code => INT16
            ...
    */
    pub fn set_status_code(
        &mut self,
        api_key: u16,
        api_version: u16,
        code: i16,
        info: &mut KafkaInfo,
    ) {
        if api_key == KAFKA_FETCH && api_version >= 7 {
            info.status_code = Some(code as i32);
            if code == 0 {
                info.status = L7ResponseStatus::Ok;
            } else {
                info.status = L7ResponseStatus::ServerError;
                self.perf_stats.as_mut().map(|p| p.inc_resp_err());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::rc::Rc;
    use std::{cell::RefCell, fs};

    use super::*;

    use crate::{
        common::{flow::PacketDirection, l7_protocol_log::L7PerfCache, MetaPacket},
        flow_generator::L7_RRT_CACHE_CAPACITY,
        utils::test::Capture,
    };

    const FILE_DIR: &str = "resources/test/flow_generator/kafka";

    fn run(name: &str) -> String {
        let capture = Capture::load_pcap(Path::new(FILE_DIR).join(name), None);
        let log_cache = Rc::new(RefCell::new(L7PerfCache::new(L7_RRT_CACHE_CAPACITY)));
        let mut packets = capture.as_meta_packets();
        if packets.is_empty() {
            return "".to_string();
        }

        let mut output: String = String::new();
        let first_dst_port = packets[0].lookup_key.dst_port;
        for packet in packets.iter_mut() {
            packet.lookup_key.direction = if packet.lookup_key.dst_port == first_dst_port {
                PacketDirection::ClientToServer
            } else {
                PacketDirection::ServerToClient
            };
            let payload = match packet.get_l4_payload() {
                Some(p) => p,
                None => continue,
            };

            let mut kafka = KafkaLog::default();
            let param = &ParseParam::new(packet as &MetaPacket, log_cache.clone(), true, true);

            let is_kafka = kafka.check_payload(payload, param);
            let info = kafka.parse_payload(payload, param);
            if let Ok(info) = info {
                match info.unwrap_single() {
                    L7ProtocolInfo::KafkaInfo(i) => {
                        output.push_str(&format!("{:?} is_kafka: {}\r\n", i, is_kafka));
                    }
                    _ => unreachable!(),
                }
            } else {
                output.push_str(&format!(
                    "{:?} is_kafka: {}\r\n",
                    KafkaInfo::default(),
                    is_kafka
                ));
            }
        }
        output
    }

    #[test]
    fn check() {
        let files = vec![("kafka.pcap", "kafka.result")];

        for item in files.iter() {
            let expected = fs::read_to_string(&Path::new(FILE_DIR).join(item.1)).unwrap();
            let output = run(item.0);

            if output != expected {
                let output_path = Path::new("actual.txt");
                fs::write(&output_path, &output).unwrap();
                assert!(
                    output == expected,
                    "output different from expected {}, written to {:?}",
                    item.1,
                    output_path
                );
            }
        }
    }

    #[test]
    fn check_perf() {
        let expected = vec![
            (
                "kafka.pcap",
                L7PerfStats {
                    request_count: 1,
                    response_count: 1,
                    err_client_count: 0,
                    err_server_count: 0,
                    err_timeout: 0,
                    rrt_count: 1,
                    rrt_sum: 4941,
                    rrt_max: 4941,
                },
            ),
            (
                "kafka_fetch.pcap",
                L7PerfStats {
                    request_count: 1,
                    response_count: 1,
                    err_client_count: 0,
                    err_server_count: 0,
                    err_timeout: 0,
                    rrt_count: 1,
                    rrt_sum: 504829,
                    rrt_max: 504829,
                },
            ),
        ];

        for item in expected.iter() {
            assert_eq!(item.1, run_perf(item.0), "parse pcap {} unexcepted", item.0);
        }
    }

    fn run_perf(pcap: &str) -> L7PerfStats {
        let rrt_cache = Rc::new(RefCell::new(L7PerfCache::new(100)));
        let mut kafka = KafkaLog::default();

        let capture = Capture::load_pcap(Path::new(FILE_DIR).join(pcap), None);
        let mut packets = capture.as_meta_packets();

        let first_dst_port = packets[0].lookup_key.dst_port;
        for packet in packets.iter_mut() {
            if packet.lookup_key.dst_port == first_dst_port {
                packet.lookup_key.direction = PacketDirection::ClientToServer;
            } else {
                packet.lookup_key.direction = PacketDirection::ServerToClient;
            }

            if packet.get_l4_payload().is_some() {
                let _ = kafka.parse_payload(
                    packet.get_l4_payload().unwrap(),
                    &ParseParam::new(&*packet, rrt_cache.clone(), true, true),
                );
            }
        }
        kafka.perf_stats.unwrap()
    }
}
