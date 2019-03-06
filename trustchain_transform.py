from binascii import hexlify


class TrustChainBlock2Dict(object):

    @staticmethod
    def transform(block):
        size = 1024 * 1024*1024
        res = {"time": block.timestamp,
               "full_from": hexlify(block.public_key),
               "full_to": hexlify(block.link_public_key),
               "block_id": block.block_id, "from": hexlify(block.public_key)[-8:],
               "seq_n": block.sequence_number, "to": hexlify(block.link_public_key)[-8:],
               "link": block.linked_block_id,
               "lseq_n": block.link_sequence_number,
               "down": str(block.transaction["down"]), "up": str(block.transaction["up"]),
               "amount": float(block.transaction["up"] - block.transaction["down"]) / size}
        return res
