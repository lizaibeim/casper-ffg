import datetime
import hashlib
import json
import os
import random
import sys
from time import time

from base_comm import comm3
from core import generate_timestamp, get_unix_zero_timestamp, get_hash_value_of_obj, OpLock, Protocol

if os.path.join(sys.path[0], '..') not in sys.path:
    sys.path.insert(1, os.path.join(sys.path[0], '..'))
if os.path.join(sys.path[0], '../..') not in sys.path:
    sys.path.insert(1, os.path.join(sys.path[0], '../..'))

NUM_VALIDATOR = 3
BLOCK_PRO_INTERVAL = 10
EPOCH_LEN = 5
AVG_LATENCY = 100
validator_set = list(range(0,NUM_VALIDATOR))    # all validators
initial_validator = list(range(0,NUM_VALIDATOR)) # set of initial validators for the genesis block


class Block():
    """Each node adds a new block to the blockchain between every block_proposal_interval(eg.100tick(s) for one block).

    Attributes:
            timestamp: current time
            transaction: transactions
            pre_hash: hash of the parent block
            height: height of the block
            pre_dynasty: the previous dynasty to commit voting
            cur_dynasty: the current dynasty to commit voting
            next_dynasty: next dynasty generated from parent's cur_dynasty
            hash: hash of the block
    """

    def __init__(self, parent_hash=None, parent_height=None, transactions=None, timestamp=None):
        """Each block needs to be signed by 2/3 previous dynasties and current dynasties.

        Args:
            parent_hash:  The hash of the parent of the block
            parent_height: The height of the parent of the block
            transactions: the transcations collected by the validator (or on the chain)
        """
        # If it is the genesis block, then initialize it
        if not parent_hash:
            self.timestamp = get_unix_zero_timestamp()
            self.transactions = []
            self.pre_hash = 0
            self.height = 0
            # self.pre_dynasty = self.cur_dynasty = Dynasty(initial_validator)
            # self.nex_dynasty = self.generate_nex_dynasty(self.cur_dynasty.id)
            self.hash = 1
            return

        # If not genesis block, set pre_hash and block_height and etc
        if not timestamp:
            self.timestamp = generate_timestamp()
        else:
            self.timestamp = timestamp

        self.transactions = transactions
        self.pre_hash = parent_hash
        self.height = parent_height + 1
        self.hash = self.__hash__()

        # # Generate next dynasty randomly by using parents' current dynasty
        # self.nex_dynasty = self.generate_nex_dynasty(parent.cur_dynasty.id)
        # # If parents' cur_dynasty is finalized(meaning the term of one set of validaotrs has expired), then shift to the next dynasty
        # if parent.cur_dynasty in final_dynasties:
        #     self.pre_dynasty = parent.cur_dynasty
        #     self.cur_dynasty = parent.nex_dynasty
        #     self.hash = self.__hash__()
        #     return
        # # If parent's cur_dynasty is not ye finalized, then the child's cur and pre dynasty equals to parent's
        # self.cur_dynasty = parent.cur_dynasty
        # self.pre_dynasty = parent.pre_dynasty
        # self.hash = self.__hash__()

    def block_dict(self):
        dictionary = {
            'timestamp': self.timestamp,
            'transactions': self.transactions,
            'pre_hash':self.pre_hash,
            'height':self.height,
            # 'previous dynasty':self.pre_dynasty.validators,
            # 'current dynasty':self.cur_dynasty.validators,
            # 'next dynasty':self.nex_dynasty.validators,
            'hash':self.hash
        }
        return dictionary

    @property
    def epoch(self):
        return self.height // EPOCH_LEN

    def __hash__(self):
        return hash(str(self.timestamp) + str(self.transactions) + str(self.pre_hash) + str(self.height))

    # def generate_nex_dynasty(self, dynasty_id):
    #     # Fix the seed of random, so that each validator can generate the same dyansty
    #     random.seed(random.randint(1, 10**30))
    #     nex_dynasty = Dynasty(random.sample(validator_set,NUM_VALIDATOR),dynasty_id + 1)

    #     # Remove seed
    #     random.seed()
    #     return nex_dynasty


class Dynasty():
    """A certain set of validators.

    Attributes:
        validators: set of valid validators
        id: id of the dynasty
    """

    def __init__(self, validators, id=0):
        self.validators = validators
        self.id = id

    def __hash__(self):
        return hash(str(self.id) + str(self.validators))

    def __eq__(self, compare):
        return (str(self.id) + str(self.validators) == str(compare.id) + str(compare.validators))

class Vote():
    """Voting messages: establish the supermajority link.

    Attributes:
        source_hash: hash of the source block
        target_hash: hash of the target block
        source_epoch: epoch of the source block
        target_epoch: epoch of the target block
        sender: node who sends the voting message
        hash: hash of the Vote object
    """

    def __init__(self, source_hash, target_hash, source_epoch, target_epoch, sender):
        self.source_hash = source_hash
        self.target_hash = target_hash
        self.source_epoch = source_epoch
        self.target_epoch = target_epoch
        self.sender = sender
        self.hash = self.__hash__()

    def __hash__(self):
        return hash(str(self.source_hash) + str(self.target_hash) + str(self.source_epoch) + str(self.target_epoch) + str(self.sender))

# Root of the blockchain
ROOT =  Block()

class CasperFFG(Protocol):
    # Static attributes
    configurations = {}

    def on_creation(self):
        """
        When this class is being initialized, this method will be invoked.
        Do not override the __init__ function or this module will not work.
        """
        self.block_lock = OpLock('BL') # multi-threading lock for block adding.
        self.transaction_lock = OpLock('TX') # multi-threading lock for transaction adding.

# =============================================================================
#         Initialization:
#
#         (1) chain is used to store the blockchain locally in the memory.
#         (2) current_transactions is the pending transactions which haven't been added to the main chain.
#         (3) self.new_block method creates the first block(genesis block)
# =============================================================================
# =============================================================================
#
#         Node Information:
#
#         (1) you can use methods provided by comm3 to perform peer-related operations
#
#         (2) node_ID is the unique name of the current node
#
#         >>Examples of retrieving peers:
#
#         (1) get one neighbor of this node:
#
#         comm3.get_peer(peerID='someID')
#
#         (2) get all neighbors of this node:
#
#         comm3.get_all_peers()
#
#         (3) get k random neighbors of this node:
#
#         comm3.get_k_peers(k=5)
#
#         -----------------------------------------------------------------------
#         >>Examples of sending messages:
#
#         (1) send request to one node:
#
#         comm3.send(peerID='linux-boy',route='/receive_route',params={'data':"hello linux-boy!"})
#
#         (2) broadcast requests to k nodes in neighborlist:
#
#         comm3.broadcast_to_k_neighbors_in_random(k=5,route='/receive_route',params={'data':'hello lucky homies!'})
#
#         (3) broadcast requests to every nodes in neighborlist:
#
#         comm3.broadcast_to_all_neighbors(route='/receive_route',params={'data':'hello everyone!'})
#
#         -----------------------------------------------------------------------
#         >>Examples of getting information of this node
#
#         node_ID = comm3.get_nodeID() # the name of this node
#
#         exp_ID = comm3.get_expID() # the ID of current experiment.
#
# =============================================================================

        self.chain = [] # the local chain
        self.id = -1 # the number id of this node
        self.current_transactions = [] # current transactions locally
        # self.new_block = Block(proof=100,new_block_index=1,previous_hash=1,genesis=True) # create the genesis block
        self.timestamp = generate_timestamp() # the timestamp when this chain is started.
        self.peers = comm3.get_all_peers() # the neighborlist of current setting.
        self.node_ID = comm3.get_nodeID() # the id name of this node
        self.exp_ID = comm3.get_expID() # the id of this experiment
        self.configurations = self.__conf__ # self.__conf__ contains the value of configurations passed from constructor
        # If you want to perform some operations based on certain settings. do it with self.configurations below
        # processed blocks
        self.processed = {ROOT.hash: ROOT}
        # Message that are not be processed yet (Vote or block) beacuse the lack of certain pre-requsite (hash of dependency);
        # The dependencies dictionary {hash of denpendency -> object}
        self.dependencies = {}
        # Set of finalized dynasties
        # self.final_dynasties = set()
        # self.final_dynasties.add(Dynasty(initial_validator))
        # The current epoch (binded with the last justified checkpoint the validator has voted for)
        self.current_epoch = 0
        # Tails dict {checkpoint_hash -> the last block following this checkpoint(before meeting the next checkpoint) }
        self.tails = {ROOT.hash: ROOT}
        # Tails_closest_check dict {hash of the closest ancestor checkpoint of the block -> hash of the block}
        self.tails_closest_checkpoint = {ROOT.hash: ROOT.hash}
        # The head is the latest block processed descendant of the highest justified checkpoint
        self.head = ROOT
        self.highest_justified_checkpoint = ROOT
        self.main_chain_size = 1
        # Set of justified block hashes
        self.justified = {ROOT.hash}
        # Set of finalized block hashes
        self.finalized = {ROOT.hash}
        self.chain.append(ROOT.block_dict())
        # Map {sender -> votes}
        self.votes = {}
        # Map {source_hash -> {target_hash -> count}} to count the votes
        self.vote_count = {}

    def initial_id(self):
        self.id = comm3.get_unique_number_ID()
        if self.id == -1:
            return False
        return True

    def get_closest_checkpoint(self, block):
        """Get the closest ancestor checkpoint of a given block (even the given block is a checkpoint)"""
        if block.height == 0:
            return None
        return self.processed[self.tails_closest_checkpoint[block.pre_hash]]

    def add_dependency(self, hash, object):
        """For the object could not been processed becasue of lacking certain dependency, add it into dependencies.

        Args:
            hash: hash of the dependency (e.g. vote.target_hash)
            object: (e.g. vote)
        """
        if hash not in self.dependencies:
            self.dependencies[hash] = []
        self.dependencies[hash].append(object)

    def check_ancestor(self, ancestor, descendant) :
        """compare two checkpoints whether they are parent-child relationship.
        
        Args:
            ancestor: ancestor block / hash of block (could get the block from processed[])
            descendant: descendant block / hash of block
        """
        if not isinstance(ancestor, Block):
            ancestor = self.processed[ancestor]
        if not isinstance(descendant, Block):
            descendant = self.processed[descendant]
        if (ancestor.height % EPOCH_LEN != 0):
            return "The ancestor block is not checkpoint"
        if (descendant.height % EPOCH_LEN != 0):
            return "The descendant block is not checkpoint"
        while True:
            if descendant is None:
                return False
            if descendant.hash == ancestor.hash:
                return True
            descendant = self.get_closest_checkpoint(descendant)

    def one_tick(self, time):
        """for each iterations, check whether this node need to build a block or not."""
        # new_block = Block(self.head.hash,self.head.height, self.current_transactions)
        # self.chain.append(new_block)
        # print(self.chain)
        # print("self.id:",self.id,"time:",time,"time // BLOCK_PRO_INTERVAL % NUM_VALIDATOR", (time // BLOCK_PRO_INTERVAL) % NUM_VALIDATOR, "time % block_propo:", time % BLOCK_PRO_INTERVAL)
        if self.id == (time // BLOCK_PRO_INTERVAL) % NUM_VALIDATOR and time % BLOCK_PRO_INTERVAL == 0:
            # It is time for one node to build a block
            # The head is the latest descendant block of the highest justified checkpoint
            new_block = Block(self.head.hash, self.head.height, self.current_transactions)
            # self.chain.append(new_block.block_dict())
            # print(self.chain)
            params = [self.head.hash, self.head.height, self.current_transactions, new_block.timestamp]
            self.broadcast_new_block(params)
            self.on_receive(new_block)

    def set_id(self, id):
        """Allocate id."""
        self.id = id

    def invoke_other_nodes(self, time):
        """Invoke other nodes to check whether is need to produce block."""
        route = '/one_tick'
        json_info = {
            'time': time
        }
        result_map = comm3.broadcast_to_all_neighbors(route, params=json_info)

    def broadcast_new_block(self, params):
        """A new block is propagated. if the hash value is not consistent with peers, the peer will check the whole chain of this node.

        Args:
            params: the parameters to transfer for instantiating a block
        """
        # neighbours = db.get_peers()
        route = '/casperffg/blocks/receive'
        json_info = {
            'Params': params,
            'from': self.node_ID
        }
        result_map = comm3.broadcast_to_all_neighbors(route, params=json_info)

    def check_justified(self, hash):
        """
        Check whether the hash of block exists in the justified checkpoint list. The genesis block is justified by born,
        and other checkpoint will be regarded as justified if there exists a supermajority link, from one justified checkpoint
        to a new checkpoint, then the new checkpoint will become justified.

        Args:
            hash: the hash of block

        Returns:
            True if the block is justified checkpoint, False if not
        """
        assert hash in self.processed, "Hash %d not found" % hash
        assert self.processed[hash].height % EPOCH_LEN == 0, "Not checkpoint"
        return hash in self.justified

    def check_finalized(self, hash):
        """
        Check whether the hash of block exists in the finalized checkpoint list. The genesis block is finalized by born.
        Other checkpoint will be regarded as finalized if there exists a supermajority link, from one justified checkpoint to
        its direct child checkpoint(height+1), then the source checkpoint will become finalized

        Args:
            hash: hash of block
        
        Returns:
            True if the block is finalized checkpoint, False if not
        """
        assert hash in self.processed, "Hash %d not found" % hash
        assert self.processed[hash].height % EPOCH_LEN == 0, "Not checkpoint"
        return hash in self.finalized

    @property
    def head(self):
        return self._head

    @head.setter
    def head(self, value):
        self._head = value

    def accept_block(self, block, fromnode=None):
        """
        Args:
            block: the block to be processed
            fromnode: where is the block from
        
        Returns:
            True if the block has been processed, False if it lacks some dependencies
        """
        # Use a lock to gurantee the atomic operations on chain
        self.block_lock.acquire("accept_block")

        # If we didn't receive the block's parent yet, wait
        if block.pre_hash not in self.processed:
            self.add_dependency(block.pre_hash, block)
            self.block_lock.release("accept_block")
            return False

        # We receive the block
        self.processed[block.hash] = block
        self.chain.append(block.block_dict())

        # If it's an epoch block (in general)
        if block.height % EPOCH_LEN == 0:
            #  Start a tail object for it
            self.tails_closest_checkpoint[block.hash] = block.hash
            self.tails[block.hash] = block
            # Maybe vote
            self.start_vote_establish_supermajoritylink(block) # to establish a supermajority link

        # Otherwise...
        else:
            # See if it's part of the longest tail, if so set the tail accordingly
            assert block.pre_hash in self.tails_closest_checkpoint
            # The new block is in the same tail as its parent
            self.tails_closest_checkpoint[block.hash] = self.tails_closest_checkpoint[block.pre_hash]
            # If the block has the highest height, it becomes the end of the tail
            if block.height > self.tails[self.tails_closest_checkpoint[block.hash]].height:
                self.tails[self.tails_closest_checkpoint[block.hash]] = block

        # Reorganize the head
        self.check_head(block)
        self.block_lock.release("accept_block")
        return True

    def check_head(self, block):
        """
        Reorganize the head to stay on the chain with the highest justified 
        checkpoint. If we are on wrong chain, reset the head to be the highest 
        descendent among the chains containing the highest justified checkpoint.

        Args:
            block: latest block processed
        """
        # We are on the right chain, the head is simply the latest block
        if self.check_ancestor(self.highest_justified_checkpoint,self.tails_closest_checkpoint[block.hash]):
            self.head = block
            self.main_chain_size += 1

        # Otherwise, we are not on the right chain
        else:
            # Find the highest descendant of the highest justified checkpoint
            # and set it as head
            # print('Wrong chain, reset the chain to be a descendant of the '
                  # 'highest justified checkpoint.')
            max_height = self.highest_justified_checkpoint.height
            max_descendant = self.highest_justified_checkpoint.hash
            for _hash in self.tails:
                # If the tail is descendant to the highest justified checkpoint
                if self.check_ancestor(self.highest_justified_checkpoint, _hash):
                    new_height = self.processed[_hash].height
                    if new_height > max_height:
                        max_height = new_height
                        max_descendant = _hash

            self.main_chain_size = max_height
            self.head = self.processed[max_descendant]

    def start_vote_establish_supermajoritylink(self, block):
        """Called after receiving a block.

        Implement the fork rule:
        maybe send a vote message where target is block if we are on the chain 
        containing the justified checkpoint of the highest height, and we have 
        never sent a vote for this height.

        Args:
            block: last block we processed
        """
        assert block.height % EPOCH_LEN == 0, (
            "Block {} is not a checkpoint.".format(block.hash))

        # The target will be block (which is a checkpoint)
        target_block = block
        # The source will be the justified checkpoint of greatest height
        source_block = self.highest_justified_checkpoint

        # If the block is an epoch block of a higher epoch than what we've seen so far
        # This means that it's the first time we see a checkpoint at this height
        # It also means we never voted for any other checkpoint at this height (rule 1)
        if target_block.epoch > self.current_epoch:
            assert target_block.epoch > source_block.epoch, ("target epoch: {},"
            "source epoch: {}".format(target_block.epoch, source_block.epoch))

            print('Validator %d: now in epoch %d' % (self.id, target_block.epoch))
            # Increment our epoch
            self.current_epoch = target_block.epoch

            # If the target_block is a descendent of the source_block, send
            # a vote
            if self.check_ancestor(source_block, target_block):
                # print('Validator %d: Voting %d for epoch %d with epoch source %d' %
                      # (self.id, target_block.hash, target_block.epoch,
                       # source_block.epoch))

                vote = Vote(source_block.hash,
                            target_block.hash,
                            source_block.epoch,
                            target_block.epoch,
                            self.id)

                params = [vote.source_hash, vote.target_hash, vote.source_epoch, vote.target_epoch,vote.sender]
                self.broadcast_new_vote(params)
                self.on_receive(vote)
                assert self.processed[target_block.hash]

    def accept_vote(self, vote, fromnode=None):
        """Called on receiving a vote message."""
        # print('Node %d: got a vote' % self.id, source.view, prepare.view_source,
        # prepare.blockhash, vote.blockhash in self.processed)

        # If the block has not yet been processed, wait
        if vote.source_hash not in self.processed:
            self.add_dependency(vote.source_hash, vote)

        # Check that the source is processed and justified
        # TODO: If the source is not justified, add to dependencies?
        if vote.source_hash not in self.justified:
            return False

        # If the target has not yet been processed, wait
        if vote.target_hash not in self.processed:
            self.add_dependency(vote.target_hash, vote)
            return False

        # If the target is not a descendent of the source, ignore the vote
        if not self.check_ancestor(vote.source_hash, vote.target_hash):
            return False

        # If the sender is not in the block's dynasty, ignore the vote
        # TODO: is it really vote.target? (to check dynasties)
        # TODO: reorganize dynasties like the paper
        # if vote.sender not in self.processed[vote.target].current_dynasty.validators and \
        #       vote.sender not in self.processed[vote.target].prev_dynasty.validators:
        #    return False

        # Initialize self.votes[vote.sender] if necessary
        if vote.sender not in self.votes:
            self.votes[vote.sender] = []

        # Check the slashing conditions
        for past_vote in self.votes[vote.sender]:
            if past_vote.target_epoch == vote.target_epoch:
                print('Break the slash condition 1.')
                return False

            if ((past_vote.source_epoch < vote.source_epoch and
                 past_vote.target_epoch > vote.target_epoch) or
                    (past_vote.source_epoch > vote.source_epoch and
                     past_vote.target_epoch < vote.target_epoch)):
                print('Break the slash condition 2.')
                return False

        # Add the vote to the map of votes
        self.votes[vote.sender].append(vote)

        # Add to the vote count
        if vote.source_hash not in self.vote_count:
            self.vote_count[vote.source_hash] = {}
        self.vote_count[vote.source_hash][vote.target_hash] = self.vote_count[
                                                        vote.source_hash].get(vote.target_hash, 0) + 1

        # Always the same right now
        # If there are enough votes, process them
        print("vote_count",self.vote_count[vote.source_hash][vote.target_hash],", num to meet:",(NUM_VALIDATOR * 2) // 3 )
        if (self.vote_count[vote.source_hash][vote.target_hash] > (NUM_VALIDATOR * 2) // 3):
            # Mark the target as justified
            self.justified.add(vote.target_hash)
            if vote.target_epoch > self.highest_justified_checkpoint.epoch:
                self.highest_justified_checkpoint = self.processed[vote.target_hash]

            # If the source was a direct parent of the target, the source
            # is finalized
            if vote.source_epoch == vote.target_epoch - 1:
                self.finalized.add(vote.source_hash)
        return True

    def broadcast_new_vote(self, params):
        """A new vote is propagated to all the peers on the network (if online)."""
        # neighbours = db.get_peers()
        route = '/casperffg/votes/receive'
        json_info = {
            'Params': params,
            'from': self.node_ID
        }
        result_map = comm3.broadcast_to_all_neighbors(route, params=json_info)

    def on_receive(self, object, fromnode=None):
        """
        Args:
            obj: could be vote or block
        Returns:
            False if the object has already been processed
        """
        if object.hash in self.processed:
            return False
        if isinstance(object, Block):
            o = self.accept_block(object, fromnode)
        elif isinstance(object, Vote):
           o = self.accept_vote(object, fromnode)
        # If the obeject was successfully processed (not marked as having unsatisfied dependencies)
        if o:
            self.processed[object.hash] = object
            if object.hash in self.dependencies:
                for p in self.dependencies[object.hash]:
                    self.on_receive(p, fromnode)
                del self.dependencies[object.hash]

    def setup_routes(self):
        from routes import casperffg_route_definitions
        casperffg_route_definitions(self,self.app)
