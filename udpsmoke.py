#!/usr/bin/env python3
# coding: utf-8

"""
The UDP Smoke - fast UDP ping that gathers statistics

Copyright (C) 2021 Tomas Hlavacek (tmshlvck@gmail.com)

This program is free software: you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation, either version 3 of the License, or (at your option) any later
version.
This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
You should have received a copy of the GNU General Public License along with
this program. If not, see <http://www.gnu.org/licenses/>.
"""

PAYLOAD_LEN=100
TIMEOUT = 2 # sec


import click
import logging
import asyncio
import time
import csv
import struct
from dataclasses import dataclass
import warnings
import ipaddress
import curses
import math


class DatagramEndpointProtocol(asyncio.DatagramProtocol):
  """Datagram protocol for the endpoint high-level interface."""

  def __init__(self, endpoint):
    self._endpoint = endpoint

  # Protocol methods

  def connection_made(self, transport):
    self._endpoint._transport = transport

  def connection_lost(self, exc):
    assert exc is None
    if self._endpoint._write_ready_future is not None:
      self._endpoint._write_ready_future.set_result(None)
    self._endpoint.close()

  # Datagram protocol methods

  def datagram_received(self, data, addr):
    self._endpoint.feed_datagram(data, addr)

  def error_received(self, exc):
    msg = 'Endpoint received an error: {!r}'
    warnings.warn(msg.format(exc))

  # Workflow control

  def pause_writing(self):
    assert self._endpoint._write_ready_future is None
    loop = self._endpoint._transport._loop
    self._endpoint._write_ready_future = loop.create_future()

  def resume_writing(self):
    assert self._endpoint._write_ready_future is not None
    self._endpoint._write_ready_future.set_result(None)
    self._endpoint._write_ready_future = None


# Enpoint classes

class Endpoint:
  """High-level interface for UDP enpoints.
  Can either be local or remote.
  It is initialized with an optional queue size for the incoming datagrams.
  """

  def __init__(self, queue_size=None):
    if queue_size is None:
        queue_size = 0
    self._queue = asyncio.Queue(queue_size)
    self._closed = False
    self._transport = None
    self._write_ready_future = None

  # Protocol callbacks

  def feed_datagram(self, data, addr):
    try:
      self._queue.put_nowait((data, addr))
    except asyncio.QueueFull:
      warnings.warn('Endpoint queue is full')

  def close(self):
    # Manage flag
    if self._closed:
      return
    self._closed = True
    # Wake up
    if self._queue.empty():
      self.feed_datagram(None, None)
    # Close transport
    if self._transport:
      self._transport.close()

  # User methods

  def send(self, data, addr):
    """Send a datagram to the given address."""
    if self._closed:
      raise IOError("Enpoint is closed")
    self._transport.sendto(data, addr)

  async def receive(self):
    """Wait for an incoming datagram and return it with
    the corresponding address.
    This method is a coroutine.
    """
    if self._queue.empty() and self._closed:
      raise IOError("Enpoint is closed")
    data, addr = await self._queue.get()
    if data is None:
      raise IOError("Enpoint is closed")
    return data, addr

  def abort(self):
    """Close the transport immediately."""
    if self._closed:
      raise IOError("Enpoint is closed")
    self._transport.abort()
    self.close()

  async def drain(self):
    """Drain the transport buffer below the low-water mark."""
    if self._write_ready_future is not None:
      await self._write_ready_future

  # Properties

  @property
  def address(self):
    """The endpoint address as a (host, port) tuple."""
    return self._transport.get_extra_info("socket").getsockname()

  @property
  def closed(self):
    """Indicates whether the endpoint is closed or not."""
    return self._closed


# High-level coroutines

async def open_datagram_endpoint(host, port, *, endpoint_factory=Endpoint, **kwargs):
  """Open and return a datagram endpoint.
  The default endpoint factory is the Endpoint class.
  The endpoint can be made local or remote using the remote argument.
  Extra keyword arguments are forwarded to `loop.create_datagram_endpoint`.
  """
  loop = asyncio.get_event_loop()
  endpoint = endpoint_factory()
  kwargs['local_addr'] = host, port
  kwargs['protocol_factory'] = lambda: DatagramEndpointProtocol(endpoint)
  await loop.create_datagram_endpoint(**kwargs)
  return endpoint


header = struct.Struct('!cQdI')
payload = ('*'*PAYLOAD_LEN).encode('ascii')
def genping(pid):
  return header.pack(b'p', pid, time.perf_counter(), len(payload)) + payload

def readpacket(data):
  op, pid, t, plen = header.unpack_from(data)
  pl = data[header.size:].decode('ascii')
  if len(pl) != plen:
    raise ValueError(f"Payload length {plen} differs from payload {len(pl)}.")
  return op, pid, t, pl

def genpong(pid, t, pl):
  return header.pack(b'r', pid, t, len(payload)) + payload


# Actual UDPsmoke implementation

class PeerStatus:
  __slots__ = ['lastpid', 'pending', 'sent', 'received', 'outoforder', 'prev_sent', 'prev_received', 'prev_outoforder', 'rtt_avg', 'rtt_var', 'win', 'interval', 'rtt_data']


  def __init__(self, interval):
    self.interval = interval
    self.win = int(10/self.interval)
    if self.win > 500:
      self.win = 500
    if self.win < 10:
      self.win = 10

    self.lastpid = 0
    self.pending = set()
    self.sent = 0
    self.received = 0
    self.outoforder = 0
    self.prev_sent = 0
    self.prev_received = 0
    self.prev_outoforder = 0
    self.rtt_avg = 0.0
    self.rtt_var = 0.0
    self.rtt_data = [None]*self.win

  def incSent(self, pid=None):
    self.sent += 1
    if pid:
      self.lastpid = pid
      self.pending.add(pid)

  def _updateRtt(self, rtt):
    self.rtt_data.append(rtt)
    oldestpoint = self.rtt_data.pop(0)

    if oldestpoint == None:
      self.rtt_avg = sum([x for x in self.rtt_data if x != None])/sum(1 for x in self.rtt_data if x != None)
      try:
        self.rtt_var = sum([(x-self.rtt_avg)**2 for x in self.rtt_data if x != None])/(sum(1 for x in self.rtt_data if x != None)-1)
      except:
        self.rtt_var = 0.0
    else:
      last_rtt_avg = self.rtt_avg
      self.rtt_avg += (rtt - oldestpoint)/self.win
      self.rtt_var += (rtt - oldestpoint)*(rtt - self.rtt_avg + oldestpoint - last_rtt_avg)/(self.win-1)
      if self.rtt_var < 0: # this can happen due to rounding errors
        self.rtt_var == 0.0

  def incReceived(self, pid, rtt=None):
    if self.pending and pid == min(self.pending):
      self.received += 1
    else:
      self.outoforder += 1

    if pid in self.pending:
      self.pending.remove(pid)

    if rtt:
      self._updateRtt(rtt)

  def csvRow(self, ip):
    lost = set()
    for p in self.pending:
      if (self.lastpid - p)*self.interval > TIMEOUT:
        lost.add(p)
    self.pending.difference_update(lost)

    res = (ip, int(time.time()), self.sent-self.prev_sent, self.received-self.prev_received, self.outoforder-self.prev_outoforder, len(lost), self.rtt_avg, math.sqrt(self.rtt_var))
    #res = (ip, int(time.time()), self.sent-self.prev_sent, self.received-self.prev_received, self.outoforder-self.prev_outoforder, len(lost)+len(self.pending), self.rtt_avg, math.sqrt(self.rtt_var))
    self.prev_sent = self.sent
    self.prev_received = self.received
    self.prev_outoforder = self.outoforder
    return res


async def run_server(ep, status):
  while True:
    data, addr = await ep.receive()
    ip, port, _, _ = addr
    op, pid, t, payload = readpacket(data)

    if op == b'p':
      ep.send(genpong(pid, t, payload), (ip, port))
    elif op == b'r':
      if ip in status:
        s = status[ip]
        rtt = time.perf_counter() - t 
        s.incReceived(pid, rtt*1000) # normalize to ms
      else:
        warnings.warn(f"Packet from unknown IP {ip}")
    else:
      warnings.warn(f"Malformed packet from IP {ip}")
    

async def send_pings(ip, port, ep, status, interval):
  pid = 1
  while True:
    ep.send(genping(pid),(ip, port))
    s = status[ip]
    s.incSent(pid)
    pid += 1
    await asyncio.sleep(interval)

def dump_status(status, stdscr):
  for ln, ip in enumerate(sorted(status.keys(), reverse=True)):
    s = status[ip]
    loss = (s.sent-s.received-s.outoforder)
    lossp = (100*loss/s.sent) if s.sent > 0 else 0
    args = []
    if lossp > 10:
      args.append(curses.A_STANDOUT)

    l = f'{ip} sent:{s.sent} recv:{s.received} outordr:{s.outoforder} loss:{loss} rtt_avg:{round(s.rtt_avg, 3)} ms rtt_sd:{round(math.sqrt(s.rtt_var),3)}'
    stdscr.addstr(ln, 0, l, *args)
    stdscr.clrtoeol()
  stdscr.refresh()

async def dump_status_task(status, interval):
  screen_refresh = interval*25
  if screen_refresh < 1:
    screen_refresh = 1
  if screen_refresh > 30:
    screen_refresh = interval

  try:
    stdscr = curses.initscr()

    while True:
      dump_status(status, stdscr)
      await asyncio.sleep(screen_refresh)

  finally:
    curses.endwin()


async def dump_csv_task(status, filename, interval):
  csv_refresh = interval*25
  if csv_refresh < 1:
    csv_refresh = 1
  if csv_refresh > 30:
    csv_refresh = interval

  while True:
    with open(filename, "a") as csv_file:
      wr = csv.writer(csv_file, quoting=csv.QUOTE_MINIMAL)
      for ip in status:
        wr.writerow(status[ip].csvRow(ip))

    await asyncio.sleep(csv_refresh)



async def start_all(tgtips, port, interval, csvfile):
  ep = await open_datagram_endpoint('::', port)

  tsk = []
  status = {ip:PeerStatus(interval) for ip in tgtips}
  tsk.append(asyncio.create_task(run_server(ep, status)))

  for ip in tgtips:
    tsk.append(asyncio.create_task(send_pings(ip, port, ep, status, interval)))

  tsk.append(asyncio.create_task(dump_status_task(status, interval)))
  if csvfile:
    tsk.append(asyncio.create_task(dump_csv_task(status, csvfile, interval)))

  await asyncio.gather(*tsk)


def normalize_ip(ip):
  if ipaddress.ip_address(ip).version == 4:
    return f'::ffff:{ip}'
  else:
    return ip

@click.command(help="run UDP ping with a list of remote servers and run UDP echo server")
@click.option('-v', '--verbose', 'verb', help="verbose output", is_flag=True)
@click.option('-t', '--targets', 'tgts', help="list of targets", type=click.File('r'), required=True)
@click.option('-p', '--port', 'port', help="port to use", default=54321)
@click.option('-i', '--interval', 'interval', help="time interval between pings", default=0.2)
@click.option('-c', '--csv', 'csvfile', help="CSV output", type=click.Path())
def main(verb, tgts, port, interval, csvfile):
  if verb:
    logging.basicConfig(level=logging.DEBUG)

  tgtips = [normalize_ip(l.strip()) for l in tgts.readlines()]
  port = int(port)

  asyncio.run(start_all(tgtips, port, interval, csvfile))


if __name__ == '__main__':
    main()

