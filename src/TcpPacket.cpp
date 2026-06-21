#include <litetcp/TcpPacket.h>

TcpPacket::TcpPacket() {
	// 默认构造函数：清空头部所有字节，并指定固定 20 字节头部长度
	std::memset(&header, 0, sizeof(header));
	header.hlen = sizeof(TcpHeader);
	header.plen = sizeof(TcpHeader);
}

TcpPacket::TcpPacket(uint16_t src_port, uint16_t dst_port, uint32_t seq, uint32_t ack,
					 uint8_t flags, uint16_t adv_win, const std::vector<uint8_t>& payload) {
	// 使用传入参数初始化 TCP 头部各字段
	header.source_port = src_port;
	header.destination_port = dst_port;
	header.seq_num = seq;
	header.ack_num = ack;
	header.hlen = sizeof(TcpHeader);
	// 包总长度等于头长度 (20) 加上载荷数据长度
	header.plen = sizeof(TcpHeader) + payload.size();
	header.flags = flags;
	header.advertised_window = adv_win;
	header.checksum = 0;  // 计算校验和前校验和字段需置零
	data = payload;
}

uint16_t TcpPacket::calculate_checksum(const TcpHeader& hdr, const uint8_t* data_ptr, size_t data_len) {
	uint32_t sum = 0;

	// 1. 复制头部副本，以防原报文头部已被污染，确保在计算校验和时校验和字段置为 0
	TcpHeader temp_hdr = hdr;
	temp_hdr.checksum = 0;

	// 2. 对头部以 16 位 (双字节) 为单位进行累加
	const uint16_t* hdr_ptr = reinterpret_cast<const uint16_t*>(&temp_hdr);
	for (size_t i = 0; i < sizeof(TcpHeader) / 2; ++i) {
		sum += hdr_ptr[i];
	}

	// 3. 对数据载荷以 16 位 (双字节) 为单位进行累加
	const uint16_t* data_16 = reinterpret_cast<const uint16_t*>(data_ptr);
	size_t words = data_len / 2;
	for (size_t i = 0; i < words; ++i) {
		sum += data_16[i];
	}

	// 如果数据长度是奇数，处理最后一个剩余单字节（在其右侧用 0 字节填充对齐）
	if (data_len % 2 != 0) {
		uint16_t last_word = 0;
		std::memcpy(&last_word, data_ptr + data_len - 1, 1);
		sum += last_word;
	}

	// 4. 将 32 位累加和的高 16 位与低 16 位反复相加，折叠进 16 位空间内
	while (sum >> 16) {
		sum = (sum & 0xFFFF) + (sum >> 16);
	}

	// 5. 对折叠后的二进制反码求反
	return static_cast<uint16_t>(~sum);
}

std::vector<uint8_t> TcpPacket::serialize() const {
	// 根据总长度分配输出缓冲区
	std::vector<uint8_t> buffer(header.plen);

	// 复制头部副本，并在此计算填充校验和
	TcpHeader temp_hdr = header;
	temp_hdr.checksum = calculate_checksum(temp_hdr, data.data(), data.size());

	// 拷贝头部至缓冲区前部
	std::memcpy(buffer.data(), &temp_hdr, sizeof(TcpHeader));

	// 如果存在载荷，则拷贝载荷至缓冲区后部
	if (!data.empty()) {
		std::memcpy(buffer.data() + sizeof(TcpHeader), data.data(), data.size());
	}

	return buffer;
}

bool TcpPacket::deserialize(const uint8_t* buffer, size_t length, TcpPacket& packet) {
	// 如果缓冲区包含的字节数连一个固定报头都装不下，属于坏包，直接拒绝
	if (length < sizeof(TcpHeader)) {
		return false;
	}

	// 反序列化头部并拷贝提取出来
	TcpHeader hdr;
	std::memcpy(&hdr, buffer, sizeof(TcpHeader));

	// 基础包格式校验：头长度必须合法 (固定 20)，包宣称的总长度不能超过接收缓冲区实际数据大小
	if (hdr.hlen != sizeof(TcpHeader) || hdr.plen > length) {
		return false;
	}

	size_t data_len = hdr.plen - sizeof(TcpHeader);
	const uint8_t* data_ptr = buffer + sizeof(TcpHeader);

	// 验证 16 位 Internet 校验和
	uint16_t computed = calculate_checksum(hdr, data_ptr, data_len);
	if (computed != hdr.checksum) {
		// 如果校验和不匹配，说明包已损坏，直接返回 false（丢弃该包）
		return false;
	}

	// 校验成功，填充输出包的头部和数据字段
	packet.header = hdr;
	packet.data.assign(data_ptr, data_ptr + data_len);
	return true;
}
