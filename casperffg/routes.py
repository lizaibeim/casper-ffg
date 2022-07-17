#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 25 19:36:30 2019

@author: Eric
"""

import os
import sys
import time

from base_comm import comm3
from core import return_json_response,get_params_in_request
from flask import request,render_template

from casperffg import Block, Vote

if os.path.join(sys.path[0], '..') not in sys.path:
  sys.path.insert(1, os.path.join(sys.path[0], '..'))
  
NUM_VALIDATOR = 2
BLOCK_PRO_INTERVAL = 100
EPOCH_LEN = 2
AVG_LATENCY = 100
validator_set = list(range(0,NUM_VALIDATOR))    # all validators
initial_validator = list(range(0,NUM_VALIDATOR)) # set of initial validators for the genesis block

#the blockchian object means one validator
def casperffg_route_definitions(blockchain,app):
  
  @app.route('/casperffg')
  def casperffg_index():
    # you can set exp_ID and node_ID when server is on.
    experimentId = None
    if not blockchain.exp_ID:
      experimentId = request.args.get('expID')
    if experimentId:
      blockchain.exp_ID = experimentId
    elif not blockchain.exp_ID:
      return return_json_response(request,{'Error':"[NODE]no valid experimentId provided."})
    
    node_ID = request.values.get('id')
    if node_ID:
      blockchain.node_ID = node_ID
    else:
      if not blockchain.node_ID:
        return return_json_response(request,{'Error':"node_ID is not provided."})
    return return_json_response(request,{'node_ID':blockchain.node_ID,'exp_ID':blockchain.exp_ID,'type':"CasperFFG"})

  @app.route("/casperffg/view_report", methods=["GET","POST"])
  def casper_report():
      temp = blockchain.highest_justified_checkpoint
      num_justified = 0
      num_finalized = 0
      sum = 0
      while temp is not None:
          sum += 1
          if temp.hash in blockchain.justified:
              num_justified += 1
          if temp.hash in blockchain.finalized:
              num_finalized += 1
          temp = blockchain.get_closest_checkpoint(temp)

      ratio_justified = float(num_justified) / float(sum)
      ratio_finalized = float(num_finalized) / float(sum)
      num_justified_in_fork = len(blockchain.justified) - num_justified
      ratio_forked_justified = float(num_justified_in_fork) / float(sum)
      main_chain_size = blockchain.highest_justified_checkpoint.height + 1

      response = {
          'No Of Finalized Checkpoint:': num_finalized,
          'No Of Justified Checkpoint:': num_justified,
          'No Of Justified Checkpoint In Fork': num_justified_in_fork,
          'Sum Of Checkpoint:':sum,
          'Justified Ratio:':ratio_justified,
          'Finalized Ratio:':ratio_finalized,
          'Forked Ratio:': ratio_forked_justified,
          'Main Chain Size': main_chain_size,
          'Vote counts': blockchain.vote_count
      }
      return return_json_response(request, response)

  @app.route("/casperffg/view_chain", methods=["GET", "POST"])
  def casper_view():
    response = {
          'node':blockchain.node_ID,
          'chain': blockchain.chain,
          'length': len(blockchain.chain),
          'timestamp':blockchain.timestamp
    }
    return return_json_response(request,response)

  @app.route("/casperffg/set_id", methods=["GET", "POST"])
  def casper_register():
    if blockchain.initial_id():
        resp = {
            "message": "The ID has been set",
            "id": blockchain.id
        }
    else:
        resp = {
            "message": "You have not registered yet, Id allocated fails",
            "id": blockchain.id
        }
    return  return_json_response(request, resp)

  @app.route("/one_tick", methods = ["POST"])
  def one_tick():
      params = get_params_in_request(request)
      time = params['time']
      blockchain.one_tick(time)
      return ""

  @app.route("/casperffg/simulation", methods = ["GET", "POST"])
  def casper_simulation():
      for t in range(10*5*50): # t indicating the tick of time
          start = time.time()
          blockchain.one_tick(t)
          blockchain.invoke_other_nodes(t)
      resp = {
          "message": "Small simulation successfully"
      }
      return return_json_response(request, resp)

  @app.route("/casperffg/votes/receive", methods = ["GET", "POST"])
  def casper_receive_vote():
      params = get_params_in_request(request)
      parameters = params['Params']
      from_node = params['from']
      new_vote = Vote(parameters[0],parameters[1],parameters[2],parameters[3], parameters[4])
      blockchain.on_receive(new_vote, from_node)
      resp = {
          "message":"vote is received."
      }
      return return_json_response(request, resp)

  @app.route("/casperffg/blocks/receive", methods=["GET", "POST"])
  def casper_receive_new_block():
    params = get_params_in_request(request)
    parameters = params['Params']
    from_node = params['from']
    new_block = Block(parameters[0], parameters[1], parameters[2], parameters[3])
    blockchain.on_receive(new_block, from_node)
    # traffic_collector.record_traffic(experimentId=blockchain.exp_ID,request_method='GET',fromNode=from_node,toNode=blockchain.node_ID,content=json.dumps(data),traffic_type='RECV(block)',comment='A block from {} is received by {}'.format(str(from_node),str(blockchain.node_ID)))

    resp = {
      "message": "block is received."
    }
    return return_json_response(request, resp)
