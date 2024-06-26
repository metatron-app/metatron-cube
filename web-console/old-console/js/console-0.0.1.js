/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
// requires tableHelper

var oTable = [];

var resetSupervisor = function(supervisorId) {
  if(confirm('Do you really want to reset: '+ supervisorId)) {
    $.ajax({
      type:'POST',
      url: '/druid/indexer/v1/supervisor/' + supervisorId + '/reset',
      data: ''
    }).done(function(data) {
      setTimeout(function() { location.reload(true) }, 750);
    }).fail(function(data) {
      alert('Reset request failed, please check overlord logs for details.');
    })
  }
}

var shutdownSupervisor = function(supervisorId) {
  if(confirm('Do you really want to shutdown: '+ supervisorId)) {
    $.ajax({
      type:'POST',
      url: '/druid/indexer/v1/supervisor/' + supervisorId + '/shutdown',
      data: ''
    }).done(function(data) {
      setTimeout(function() { location.reload(true) }, 750);
    }).fail(function(data) {
      alert('Shutdown request failed, please check overlord logs for details.');
    })
  }
}

$(document).ready(function() {
  var augment = function(data) {
    for (i = 0 ; i < data.length ; i++) {
      var taskId = encodeURIComponent(data[i].id)
      data[i].more =
        '<a href="/druid/indexer/v1/task/' + taskId + '">payload</a>' +
        '<a href="/druid/indexer/v1/task/' + taskId + '/status">status</a>' +
        '<a href="/druid/indexer/v1/task/' + taskId + '/log">log (all)</a>' +
        '<a href="/druid/indexer/v1/task/' + taskId + '/log?offset=-8192">log (last 8kb)</a>'
    }
  }

  $.get('/druid/indexer/v1/supervisor', function(dataList) {
    var data = []
    for (i = 0 ; i < dataList.length ; i++) {
      var supervisorId = encodeURIComponent(dataList[i])
      data[i] = {
        "dataSource" : supervisorId,
        "more" :
          '<a href="/druid/indexer/v1/supervisor/' + supervisorId + '">payload</a>' +
          '<a href="/druid/indexer/v1/supervisor/' + supervisorId + '/status">status</a>' +
          '<a href="/druid/indexer/v1/supervisor/' + supervisorId + '/history">history</a>' +
          '<a onclick="resetSupervisor(\'' + supervisorId + '\');">reset</a>' +
          '<a onclick="shutdownSupervisor(\'' + supervisorId + '\');">shutdown</a>'
      }
    }
    buildTable((data), $('#supervisorsTable'));
    if (dataList.length > 0) {
      $('.supervisors_section').show();
    }
  });

  $.get('/druid/indexer/v1/runningTasks', function(data) {
    $('.running_loading').hide();
    augment(data);
    buildTable(data, $('#runningTable'));
  });

  $.get('/druid/indexer/v1/pendingTasks', function(data) {
    $('.pending_loading').hide();
    augment(data);
    buildTable(data, $('#pendingTable'));
  });

  $.get('/druid/indexer/v1/waitingTasks', function(data) {
    $('.waiting_loading').hide();
    augment(data);
    buildTable(data, $('#waitingTable'));
  });

  $.get('/druid/indexer/v1/completeTasks', function(data) {
    $('.complete_loading').hide();
    augment(data);
    buildTable(data, $('#completeTable'));
  });

  $.get('/druid/indexer/v1/workers', function(data) {
    $('.workers_loading').hide();
    buildTable(data, $('#workerTable'));
  });

  $.get('/druid/indexer/v1/scaling', function(data) {
    $('.events_loading').hide();
    buildTable(data, $('#eventTable'));
  });
});
