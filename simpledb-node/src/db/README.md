# LSM Schema Design

<h2>VLog Entry</h2>

| Name  | # of bytes | Go type |
| -------| ------ | -------- |
| keySize  | 1  | uint8 |
| key  | 1-255  | string |
| valueSize | 2 | uint16 |
| value | 1 - 65,535 | string |

<h2>LSM Data Block Entry</h2>

| Name  | # of bytes | Go type |
| -------| ------ | -------- |
| keySize  | 1  | uint8 |
| key  | 1-255  | string |
| valueOffset | 8 | uint64 |
| valueSize | 4 | uint32 |

<h2>LSM Index Block Entry</h2>

| Name  | # of bytes | Go type |
| -------| ------ | -------- |
| keySize  | 1  | uint8 |
| key  | 1-255  | string |
| offset | 4 | uint32 |
