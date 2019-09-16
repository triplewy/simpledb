// Code generated by protoc-gen-go. DO NOT EDIT.
// source: simpledb.proto

package simpledb

import (
	context "context"
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

type Empty struct {
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *Empty) Reset()         { *m = Empty{} }
func (m *Empty) String() string { return proto.CompactTextString(m) }
func (*Empty) ProtoMessage()    {}
func (*Empty) Descriptor() ([]byte, []int) {
	return fileDescriptor_748391160b9263c4, []int{0}
}

func (m *Empty) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Empty.Unmarshal(m, b)
}
func (m *Empty) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Empty.Marshal(b, m, deterministic)
}
func (m *Empty) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Empty.Merge(m, src)
}
func (m *Empty) XXX_Size() int {
	return xxx_messageInfo_Empty.Size(m)
}
func (m *Empty) XXX_DiscardUnknown() {
	xxx_messageInfo_Empty.DiscardUnknown(m)
}

var xxx_messageInfo_Empty proto.InternalMessageInfo

type RemoteNodeMsg struct {
	Addr                 string   `protobuf:"bytes,1,opt,name=addr,proto3" json:"addr,omitempty"`
	Id                   []byte   `protobuf:"bytes,2,opt,name=id,proto3" json:"id,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *RemoteNodeMsg) Reset()         { *m = RemoteNodeMsg{} }
func (m *RemoteNodeMsg) String() string { return proto.CompactTextString(m) }
func (*RemoteNodeMsg) ProtoMessage()    {}
func (*RemoteNodeMsg) Descriptor() ([]byte, []int) {
	return fileDescriptor_748391160b9263c4, []int{1}
}

func (m *RemoteNodeMsg) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_RemoteNodeMsg.Unmarshal(m, b)
}
func (m *RemoteNodeMsg) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_RemoteNodeMsg.Marshal(b, m, deterministic)
}
func (m *RemoteNodeMsg) XXX_Merge(src proto.Message) {
	xxx_messageInfo_RemoteNodeMsg.Merge(m, src)
}
func (m *RemoteNodeMsg) XXX_Size() int {
	return xxx_messageInfo_RemoteNodeMsg.Size(m)
}
func (m *RemoteNodeMsg) XXX_DiscardUnknown() {
	xxx_messageInfo_RemoteNodeMsg.DiscardUnknown(m)
}

var xxx_messageInfo_RemoteNodeMsg proto.InternalMessageInfo

func (m *RemoteNodeMsg) GetAddr() string {
	if m != nil {
		return m.Addr
	}
	return ""
}

func (m *RemoteNodeMsg) GetId() []byte {
	if m != nil {
		return m.Id
	}
	return nil
}

type RemoteNodesReplyMsg struct {
	RemoteNodes          []*RemoteNodeMsg `protobuf:"bytes,1,rep,name=remoteNodes,proto3" json:"remoteNodes,omitempty"`
	XXX_NoUnkeyedLiteral struct{}         `json:"-"`
	XXX_unrecognized     []byte           `json:"-"`
	XXX_sizecache        int32            `json:"-"`
}

func (m *RemoteNodesReplyMsg) Reset()         { *m = RemoteNodesReplyMsg{} }
func (m *RemoteNodesReplyMsg) String() string { return proto.CompactTextString(m) }
func (*RemoteNodesReplyMsg) ProtoMessage()    {}
func (*RemoteNodesReplyMsg) Descriptor() ([]byte, []int) {
	return fileDescriptor_748391160b9263c4, []int{2}
}

func (m *RemoteNodesReplyMsg) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_RemoteNodesReplyMsg.Unmarshal(m, b)
}
func (m *RemoteNodesReplyMsg) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_RemoteNodesReplyMsg.Marshal(b, m, deterministic)
}
func (m *RemoteNodesReplyMsg) XXX_Merge(src proto.Message) {
	xxx_messageInfo_RemoteNodesReplyMsg.Merge(m, src)
}
func (m *RemoteNodesReplyMsg) XXX_Size() int {
	return xxx_messageInfo_RemoteNodesReplyMsg.Size(m)
}
func (m *RemoteNodesReplyMsg) XXX_DiscardUnknown() {
	xxx_messageInfo_RemoteNodesReplyMsg.DiscardUnknown(m)
}

var xxx_messageInfo_RemoteNodesReplyMsg proto.InternalMessageInfo

func (m *RemoteNodesReplyMsg) GetRemoteNodes() []*RemoteNodeMsg {
	if m != nil {
		return m.RemoteNodes
	}
	return nil
}

type HostStatsReplyMsg struct {
	TotalMemory          string   `protobuf:"bytes,1,opt,name=totalMemory,proto3" json:"totalMemory,omitempty"`
	UsedMemory           string   `protobuf:"bytes,2,opt,name=usedMemory,proto3" json:"usedMemory,omitempty"`
	TotalSpace           string   `protobuf:"bytes,3,opt,name=totalSpace,proto3" json:"totalSpace,omitempty"`
	UsedSpace            string   `protobuf:"bytes,4,opt,name=usedSpace,proto3" json:"usedSpace,omitempty"`
	NumCores             string   `protobuf:"bytes,5,opt,name=numCores,proto3" json:"numCores,omitempty"`
	CpuPercent           string   `protobuf:"bytes,6,opt,name=cpuPercent,proto3" json:"cpuPercent,omitempty"`
	Os                   string   `protobuf:"bytes,7,opt,name=os,proto3" json:"os,omitempty"`
	Hostname             string   `protobuf:"bytes,8,opt,name=hostname,proto3" json:"hostname,omitempty"`
	Uptime               string   `protobuf:"bytes,9,opt,name=uptime,proto3" json:"uptime,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *HostStatsReplyMsg) Reset()         { *m = HostStatsReplyMsg{} }
func (m *HostStatsReplyMsg) String() string { return proto.CompactTextString(m) }
func (*HostStatsReplyMsg) ProtoMessage()    {}
func (*HostStatsReplyMsg) Descriptor() ([]byte, []int) {
	return fileDescriptor_748391160b9263c4, []int{3}
}

func (m *HostStatsReplyMsg) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_HostStatsReplyMsg.Unmarshal(m, b)
}
func (m *HostStatsReplyMsg) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_HostStatsReplyMsg.Marshal(b, m, deterministic)
}
func (m *HostStatsReplyMsg) XXX_Merge(src proto.Message) {
	xxx_messageInfo_HostStatsReplyMsg.Merge(m, src)
}
func (m *HostStatsReplyMsg) XXX_Size() int {
	return xxx_messageInfo_HostStatsReplyMsg.Size(m)
}
func (m *HostStatsReplyMsg) XXX_DiscardUnknown() {
	xxx_messageInfo_HostStatsReplyMsg.DiscardUnknown(m)
}

var xxx_messageInfo_HostStatsReplyMsg proto.InternalMessageInfo

func (m *HostStatsReplyMsg) GetTotalMemory() string {
	if m != nil {
		return m.TotalMemory
	}
	return ""
}

func (m *HostStatsReplyMsg) GetUsedMemory() string {
	if m != nil {
		return m.UsedMemory
	}
	return ""
}

func (m *HostStatsReplyMsg) GetTotalSpace() string {
	if m != nil {
		return m.TotalSpace
	}
	return ""
}

func (m *HostStatsReplyMsg) GetUsedSpace() string {
	if m != nil {
		return m.UsedSpace
	}
	return ""
}

func (m *HostStatsReplyMsg) GetNumCores() string {
	if m != nil {
		return m.NumCores
	}
	return ""
}

func (m *HostStatsReplyMsg) GetCpuPercent() string {
	if m != nil {
		return m.CpuPercent
	}
	return ""
}

func (m *HostStatsReplyMsg) GetOs() string {
	if m != nil {
		return m.Os
	}
	return ""
}

func (m *HostStatsReplyMsg) GetHostname() string {
	if m != nil {
		return m.Hostname
	}
	return ""
}

func (m *HostStatsReplyMsg) GetUptime() string {
	if m != nil {
		return m.Uptime
	}
	return ""
}

type RpcOkayMsg struct {
	Ok                   bool     `protobuf:"varint,1,opt,name=Ok,proto3" json:"Ok,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *RpcOkayMsg) Reset()         { *m = RpcOkayMsg{} }
func (m *RpcOkayMsg) String() string { return proto.CompactTextString(m) }
func (*RpcOkayMsg) ProtoMessage()    {}
func (*RpcOkayMsg) Descriptor() ([]byte, []int) {
	return fileDescriptor_748391160b9263c4, []int{4}
}

func (m *RpcOkayMsg) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_RpcOkayMsg.Unmarshal(m, b)
}
func (m *RpcOkayMsg) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_RpcOkayMsg.Marshal(b, m, deterministic)
}
func (m *RpcOkayMsg) XXX_Merge(src proto.Message) {
	xxx_messageInfo_RpcOkayMsg.Merge(m, src)
}
func (m *RpcOkayMsg) XXX_Size() int {
	return xxx_messageInfo_RpcOkayMsg.Size(m)
}
func (m *RpcOkayMsg) XXX_DiscardUnknown() {
	xxx_messageInfo_RpcOkayMsg.DiscardUnknown(m)
}

var xxx_messageInfo_RpcOkayMsg proto.InternalMessageInfo

func (m *RpcOkayMsg) GetOk() bool {
	if m != nil {
		return m.Ok
	}
	return false
}

func init() {
	proto.RegisterType((*Empty)(nil), "simpledb.Empty")
	proto.RegisterType((*RemoteNodeMsg)(nil), "simpledb.RemoteNodeMsg")
	proto.RegisterType((*RemoteNodesReplyMsg)(nil), "simpledb.RemoteNodesReplyMsg")
	proto.RegisterType((*HostStatsReplyMsg)(nil), "simpledb.HostStatsReplyMsg")
	proto.RegisterType((*RpcOkayMsg)(nil), "simpledb.RpcOkayMsg")
}

func init() { proto.RegisterFile("simpledb.proto", fileDescriptor_748391160b9263c4) }

var fileDescriptor_748391160b9263c4 = []byte{
	// 357 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x74, 0x52, 0xcd, 0x6e, 0xe2, 0x30,
	0x10, 0x56, 0xcc, 0x5f, 0x18, 0x76, 0xb3, 0xc2, 0x2b, 0xed, 0x5a, 0x94, 0x56, 0x51, 0x4e, 0x9c,
	0x38, 0xc0, 0xa9, 0xc7, 0x8a, 0x56, 0xd0, 0x03, 0x05, 0x85, 0x27, 0x08, 0xc9, 0xb4, 0x8d, 0x48,
	0x62, 0x2b, 0x76, 0x0e, 0x3c, 0x47, 0x0f, 0x7d, 0xdd, 0xca, 0x4e, 0xc0, 0x54, 0x6d, 0x6f, 0xf9,
	0x7e, 0xe6, 0xcb, 0xe8, 0x1b, 0x83, 0x27, 0xd3, 0x5c, 0x64, 0x98, 0xec, 0xa7, 0xa2, 0xe4, 0x8a,
	0x53, 0xf7, 0x84, 0x83, 0x1e, 0x74, 0x1e, 0x72, 0xa1, 0x8e, 0xc1, 0x1c, 0x7e, 0x87, 0x98, 0x73,
	0x85, 0x4f, 0x3c, 0xc1, 0xb5, 0x7c, 0xa1, 0x14, 0xda, 0x51, 0x92, 0x94, 0xcc, 0xf1, 0x9d, 0x49,
	0x3f, 0x34, 0xdf, 0xd4, 0x03, 0x92, 0x26, 0x8c, 0xf8, 0xce, 0xe4, 0x57, 0x48, 0xd2, 0x24, 0xd8,
	0xc2, 0x5f, 0x3b, 0x24, 0x43, 0x14, 0xd9, 0x51, 0x8f, 0xde, 0xc2, 0xa0, 0xb4, 0x34, 0x73, 0xfc,
	0xd6, 0x64, 0x30, 0xfb, 0x3f, 0x3d, 0x2f, 0xf1, 0xe9, 0x47, 0xe1, 0xa5, 0x37, 0x78, 0x23, 0x30,
	0x5c, 0x71, 0xa9, 0x76, 0x2a, 0x52, 0x36, 0xd0, 0x87, 0x81, 0xe2, 0x2a, 0xca, 0xd6, 0x98, 0xf3,
	0xf2, 0xd8, 0xac, 0x74, 0x49, 0xd1, 0x1b, 0x80, 0x4a, 0x62, 0xd2, 0x18, 0x88, 0x31, 0x5c, 0x30,
	0x5a, 0x37, 0xf6, 0x9d, 0x88, 0x62, 0x64, 0xad, 0x5a, 0xb7, 0x0c, 0x1d, 0x43, 0x5f, 0xbb, 0x6b,
	0xb9, 0x6d, 0x64, 0x4b, 0xd0, 0x11, 0xb8, 0x45, 0x95, 0x2f, 0x78, 0x89, 0x92, 0x75, 0x8c, 0x78,
	0xc6, 0x3a, 0x39, 0x16, 0xd5, 0x16, 0xcb, 0x18, 0x0b, 0xc5, 0xba, 0x75, 0xb2, 0x65, 0x74, 0x67,
	0x5c, 0xb2, 0x9e, 0xe1, 0x09, 0x97, 0x3a, 0xeb, 0x95, 0x4b, 0x55, 0x44, 0x39, 0x32, 0xb7, 0xce,
	0x3a, 0x61, 0xfa, 0x0f, 0xba, 0x95, 0x50, 0x69, 0x8e, 0xac, 0x6f, 0x94, 0x06, 0x05, 0x63, 0x80,
	0x50, 0xc4, 0x9b, 0x43, 0x64, 0xda, 0xf0, 0x80, 0x6c, 0x0e, 0xa6, 0x04, 0x37, 0x24, 0x9b, 0xc3,
	0xec, 0xdd, 0x01, 0x77, 0x67, 0xba, 0xbd, 0xdf, 0xd3, 0x3b, 0x18, 0x2e, 0x51, 0xe9, 0x0a, 0x1f,
	0x8b, 0x67, 0xbe, 0x88, 0xb2, 0x0c, 0x4b, 0xfa, 0xc7, 0x76, 0x6f, 0xae, 0x3d, 0xba, 0xb2, 0xc4,
	0xd7, 0xb6, 0x57, 0xe0, 0x2d, 0x51, 0x99, 0x7b, 0x34, 0xf3, 0x3f, 0xdd, 0x6e, 0x74, 0xfd, 0x9d,
	0x70, 0x4e, 0xda, 0x77, 0xcd, 0x73, 0x9b, 0x7f, 0x04, 0x00, 0x00, 0xff, 0xff, 0x1a, 0x95, 0x5c,
	0x64, 0x80, 0x02, 0x00, 0x00,
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// SimpleDbClient is the client API for SimpleDb service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type SimpleDbClient interface {
	GetHostInfoCaller(ctx context.Context, in *Empty, opts ...grpc.CallOption) (*HostStatsReplyMsg, error)
	GetNodesCaller(ctx context.Context, in *RemoteNodeMsg, opts ...grpc.CallOption) (*RemoteNodesReplyMsg, error)
}

type simpleDbClient struct {
	cc *grpc.ClientConn
}

func NewSimpleDbClient(cc *grpc.ClientConn) SimpleDbClient {
	return &simpleDbClient{cc}
}

func (c *simpleDbClient) GetHostInfoCaller(ctx context.Context, in *Empty, opts ...grpc.CallOption) (*HostStatsReplyMsg, error) {
	out := new(HostStatsReplyMsg)
	err := c.cc.Invoke(ctx, "/simpledb.SimpleDb/GetHostInfoCaller", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *simpleDbClient) GetNodesCaller(ctx context.Context, in *RemoteNodeMsg, opts ...grpc.CallOption) (*RemoteNodesReplyMsg, error) {
	out := new(RemoteNodesReplyMsg)
	err := c.cc.Invoke(ctx, "/simpledb.SimpleDb/GetNodesCaller", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// SimpleDbServer is the server API for SimpleDb service.
type SimpleDbServer interface {
	GetHostInfoCaller(context.Context, *Empty) (*HostStatsReplyMsg, error)
	GetNodesCaller(context.Context, *RemoteNodeMsg) (*RemoteNodesReplyMsg, error)
}

// UnimplementedSimpleDbServer can be embedded to have forward compatible implementations.
type UnimplementedSimpleDbServer struct {
}

func (*UnimplementedSimpleDbServer) GetHostInfoCaller(ctx context.Context, req *Empty) (*HostStatsReplyMsg, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetHostInfoCaller not implemented")
}
func (*UnimplementedSimpleDbServer) GetNodesCaller(ctx context.Context, req *RemoteNodeMsg) (*RemoteNodesReplyMsg, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetNodesCaller not implemented")
}

func RegisterSimpleDbServer(s *grpc.Server, srv SimpleDbServer) {
	s.RegisterService(&_SimpleDb_serviceDesc, srv)
}

func _SimpleDb_GetHostInfoCaller_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SimpleDbServer).GetHostInfoCaller(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/simpledb.SimpleDb/GetHostInfoCaller",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SimpleDbServer).GetHostInfoCaller(ctx, req.(*Empty))
	}
	return interceptor(ctx, in, info, handler)
}

func _SimpleDb_GetNodesCaller_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RemoteNodeMsg)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SimpleDbServer).GetNodesCaller(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/simpledb.SimpleDb/GetNodesCaller",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SimpleDbServer).GetNodesCaller(ctx, req.(*RemoteNodeMsg))
	}
	return interceptor(ctx, in, info, handler)
}

var _SimpleDb_serviceDesc = grpc.ServiceDesc{
	ServiceName: "simpledb.SimpleDb",
	HandlerType: (*SimpleDbServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "GetHostInfoCaller",
			Handler:    _SimpleDb_GetHostInfoCaller_Handler,
		},
		{
			MethodName: "GetNodesCaller",
			Handler:    _SimpleDb_GetNodesCaller_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "simpledb.proto",
}
