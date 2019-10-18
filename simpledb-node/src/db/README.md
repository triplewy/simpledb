# LSM Schema Design

<h3>VLog Entry</h3>

| Name  | # of bytes | Go type |
| -------| ------ | -------- |
| keySize  | 1  | uint8 |
| key  | 1-123  | string |
| valueSize | 2 | uint16 |
| value | 1 - 65,535 | string |

<h3>LSM Data Block</h3>

| Name  | # of bytes | Go type |
| -------| ------ | -------- |
| keySize  | 1  | uint8 |
| key  | 1-123  | string |
| valueOffset | 8 | uint64 |
| valueSize | 4 | uint32 |

<b>Size: 4KB</b>

<h3>LSM Bloom Filter Block</h3>

4096 bits

<b>Size: 4KB</b>

<h3>LSM Index Block</h3>

| Name  | # of bytes | Go type |
| -------| ------ | -------- |
| keySize  | 1  | uint8 |
| key  | 1-255  | string |
| block | 4 | uint32 |

<b>Size: 16KB</b>


<p>If max size of Index Block entry is 258 bytes, we can fit ~ 64 index entries in 16 KB block. This means we
can store a maximum amount of 64 keys in an SST File, resulting in 64*268=16 KB worth of data. If a 16 KB Index Block only supports 16 KB worth of data, what is the point of an Index Block?</p>

<p>If max size of Index Block entry is now 514 bytes, we can fit ~ 32 index entries in 16 KB block. This means we can store a maximum amount of 32 Data Blocks in an SST File. If each Data Block is 4KB, then an SST File can store 128 KB worth of data.</p>

<p>Still, the WiscKey paper states that usually Level 0 SST Files are 2 MB. 128 KB of data is nowhere near that. How do we make a 16 KB index block adequately index a 2 MB file? My only guess is larger data blocks but the paper also states that data blocks are only 4 KB... Perhaps this has to do with bloom filters?</p>

| Name  | # of bytes | Go type |
| -------| ------ | -------- |
| keySize | 1 | uint8 |
| key  | 1-123  | string |
| block | 4 | uint32 |

<h3>Level sizes</h3>
<ul>
    <li>
        <b>L0</b>
        <ul>
            <li>SST File Size: 512 KB (128 Data Blocks, 1 Index Data Block)</li>
            <li>Total Level Size: 8 files</li>
        </ul>
    </li>
    <li>
        <b>L1</b>
        <ul>
            <li>SST File Size: 10 MB</li>
            <li>Total level size: 8 files</li>
        </ul>
    </li>
    <li>
        <b>L2</b>
        <ul>
            <li>SST File Size: 100 MB</li>
            <li>Total level size: 8 files</li>
        </ul>
    </li>
</ul>