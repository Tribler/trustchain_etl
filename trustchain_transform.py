from binascii import hexlify


class TrustChainBlock2Dict(object):

    @staticmethod
    def transform(block):
        res = {"time": block.timestamp,
               "block_id": block.block_id, "from": hexlify(block.public_key)[-8:],
               "seq_n": block.sequence_number, "to": hexlify(block.link_public_key)[-8:],
               "lseq_n": block.link_sequence_number,
               "down": block.transaction["down"], "up": block.transaction["up"],
               "amount": block.transaction["up"]-block.transaction["down"]}
        return res
