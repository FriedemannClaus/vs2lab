import random
import logging
import time

# coordinator messages
from const3PC import VOTE_REQUEST, PREPARE_COMMIT, GLOBAL_COMMIT, GLOBAL_ABORT
# participant decissions
from const3PC import LOCAL_SUCCESS, LOCAL_ABORT
# participant messages
from const3PC import VOTE_COMMIT, READY_COMMIT, VOTE_ABORT
# misc constants
from const3PC import TIMEOUT
# new coordinator
from const3PC import NEW_COORDINATOR

import stablelog


class Participant:
    """
    Implements a three phase commit participant.
    - state written to stable log (but recovery is not considered)
    - in case of coordinator crash, a new coordinator will be chosen, if needed
    - system does not blocks if all participants vote commit and coordinator crashes
    """

    def __init__(self, chan):
        self.channel = chan
        self.participant = self.channel.join('participant')
        self.stable_log = stablelog.create_log(
            "participant-" + self.participant)
        self.logger = logging.getLogger("vs2lab.lab6.3pc.Participant")
        self.coordinator = {}
        self.all_participants = {}
        self.state = 'NEW'
        self.decision = None
        self.newCoordFound = False

    @staticmethod
    def _do_work():
        # Simulate local activities that may succeed or not
        return LOCAL_ABORT if random.random() > 2/3 else LOCAL_SUCCESS

    def _enter_state(self, state):
        self.stable_log.info(state)  # Write to recoverable persistant log file
        self.logger.info("Participant %s entered state %s.",self.participant, state)
        self.state = state

    def init(self):
        self.channel.bind(self.participant)
        self.coordinator = self.channel.subgroup('coordinator')
        self.all_participants = self.channel.subgroup('participant')
        self._enter_state('INIT')  # Start in local INIT state.

    def run(self):
        while True:
            # get the original or new coordinator
            self.coordinator = self.channel.subgroup('coordinator')
            msg = self.channel.receive_from(self.coordinator, TIMEOUT)
            if msg:
                #print("Participant " + self.participant + " received " + str(msg))
                if msg[1]  == VOTE_REQUEST:
                    if self.state == 'INIT':
                        # Firstly, come to a local decision
                        self.decision = self._do_work() # proceed with local activities
                        self.logger.info("Participant %s made local decision %s",\
                            self.participant, self.decision)
                        if self.decision ==  LOCAL_SUCCESS:
                            self._enter_state('READY')
                            self.channel.send_to(self.coordinator, VOTE_COMMIT)
                        else:
                            self._enter_state('ABORT')
                            self.channel.send_to(self.coordinator, VOTE_ABORT)
                    else:
                        # we already made our vote but the new coordinator asks for the decision again
                        if self.state in ('READY', 'PRECOMMIT', 'COMMIT'):
                            self.channel.send_to(self.coordinator, VOTE_COMMIT)
                        else:
                            self.channel.send_to(self.coordinator, VOTE_ABORT)
                elif msg[1] == PREPARE_COMMIT:
                    if self.state == 'READY':
                        self.channel.send_to(self.coordinator, READY_COMMIT)
                        self._enter_state('PRECOMMIT')
                    elif self.state in ('PRECOMMIT','COMMIT'):
                        # if coordinatror crashed in precommit and all local decisions are success
                        self._enter_state('PRECOMMIT')
                        self.channel.send_to(self.coordinator, READY_COMMIT)
                elif msg[1] == GLOBAL_COMMIT:
                    self._enter_state('COMMIT')
                    return f"Participant {self.participant} terminated in state {self.state} (own decision: {self.decision})."
                elif msg[1] == GLOBAL_ABORT:
                    self._enter_state('ABORT')
                    return f"Participant {self.participant} terminated in state {self.state} (own decision: {self.decision})."
            else:   # no msg received means coordinator timed out
                if self.state == 'INIT':
                    self.decision = LOCAL_ABORT
                    self._enter_state('ABORT')
                    return f"Participant {self.participant} terminated in state {self.state} (own decision: {self.decision})."
                else:
                    if not self.newCoordFound:
                        if self.participant == self.findNewCoordinator():
                            # this process is the new coord
                            return self.newCoordinator()
                        # else this process is not the new coord go back to normal buisness

    def findNewCoordinator(self):
        smallestID = min(list(self.all_participants)) # get smallest proc ID
        otherParticipants = self.all_participants.copy()
        otherParticipants.remove(self.participant)

        self.channel.send_to(otherParticipants, (NEW_COORDINATOR, smallestID))

        yet_to_receive = list(otherParticipants)
        while len(yet_to_receive) > 0:
            msg = self.channel.receive_from(otherParticipants, TIMEOUT)
            if not msg:
                # participant crashed or timeout
                return f"participant {yet_to_receive[0]} didn't respond"
            else:
                assert msg[1][0] == NEW_COORDINATOR and msg[1][1] == smallestID
                yet_to_receive.remove(msg[0])
        self.newCoordFound = True
        return smallestID # all processes found the same smallest ID

    def newCoordinator(self):
        # takeover as coordinator
        self.coordinator = self.channel.join('coordinator')
        self.channel.bind(self.coordinator)
        self.logger.info("Participant %s took over as Coordinator", self.participant)

        # create a new participants list without ourself
        participants = self.all_participants.copy()
        participants.remove(self.participant)

        time.sleep(TIMEOUT) # give others time to get the new Coordinator

        #print("coord state: " + str(self.state))
        if self.state == 'READY':
            # if we are in ready state that means there was a previous vote with the old coordinator.
            # now we do another one
            self.channel.send_to(participants, VOTE_REQUEST)
            yet_to_receive = list(participants)
            while len(yet_to_receive) > 0:
                msg = self.channel.receive_from(participants, TIMEOUT)
                if not msg or msg[1] == VOTE_ABORT:
                    reason = "timeout" if not msg else "local_abort from " + msg[0]
                    self._enter_state('ABORT')
                    # something went wrong during the collection fo the votes -> global abort
                    self.channel.send_to(participants, GLOBAL_ABORT)
                    return f"Coordinator {self.participant} terminated in state ABORT. Reason: {reason}."
                assert msg[1] == VOTE_COMMIT
                yet_to_receive.remove(msg[0])
            # only received commit votes -> next state
            self._enter_state('PRECOMMIT')
        if self.state == 'PRECOMMIT':
            # Inform all participants about prepare commit
            self.channel.send_to(participants, PREPARE_COMMIT)
            # Collect ready_commit from all participants
            yet_to_receive = list(participants)
            while len(yet_to_receive) > 0:
                msg = self.channel.receive_from(participants, 2*TIMEOUT)
                if msg:
                    assert msg[1] == READY_COMMIT
                    yet_to_receive.remove(msg[0])
                else:
                    return "timeout while new coordinator was waiting for ready_commit msg"              
            self.channel.send_to(participants, GLOBAL_COMMIT)
            self._enter_state('COMMIT')
        if self.state == 'ABORT':
            self.channel.send_to(participants, GLOBAL_ABORT)
        if self.state == 'COMMIT':
            self.channel.send_to(participants, GLOBAL_COMMIT)
        return f"NewCoordinator {self.participant} terminates in state {self.state} (own decision: {self.decision})."
