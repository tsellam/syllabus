
function randInt(min, max) {
  min = Math.ceil(min);
  max = Math.floor(max);
  return Math.floor(Math.random() * (max - min)) + min;
}

function randBool() {
  return Math.random() >= 0.5;
}
function randExp() {
  return Math.pow(10, randInt(1, 6));
}
function randItem(arr) {
  return arr[randInt(0, arr.length)];
}

function sum(numbers) {
  return _.reduce(numbers, function(v1, v2) { return v1+v2; }, 0)
}

function genTransaction(opts) {
  var xact = opts.xact;
  var nobjs = opts.nobjs || 2;
  var nopts = opts.nopts || 5;
  nopts = randInt(3, nopts);
  objs = _.times(nobjs, function(i) { return i; });

  return _.times(nopts, function() {
    return {
      xact: xact,
      obj: "ABCDEF"[randItem(objs)],
      op: randItem(["R", "W"]),
      val: null
    }
  });
}

function genTransactions(opts) {
  var ops = _.flatten(_.times(2, function(idx) {
    var o = _.clone(opts);
    o.xact = "T"+(idx+1);
    return genTransaction(o);
  }));
  _.each(ops, function(op, i) {
    if (op.op == "W") {
      op.val = i;
    }
  });
  return ops;
}

function genSchedule(schedule) {
  var t1 = pickXact(schedule, "T1"),
      t2 = pickXact(schedule, "T2");
  var ret = [];
  while(_.size(t1) > 0 && _.size(t2) > 0) {
    var t = randItem([t1, t2]);
    ret.push(t.shift())
  }
  ret = _.flatten(ret.concat(_.shuffle([t1, t2])));
  return ret;
}

function genInitDB(schedule) {
  var db = {};
  _.each(schedule, function(op) {
    if (!(op.obj in db)) {
      db[op.obj] = randInt(0, 50);
    }
  });
  return db;
}

function execute(schedule, dbstate) {
  dbstate = _.clone(dbstate);
  _.each(schedule, function(op) {
    if (op.op == "W") {
      dbstate[op.obj] = op.val;
    }
  });
  return dbstate;
}

function pickXact(schedule, xact) {
  return _.filter(schedule, function(op) { return op.xact == xact; });
}

function isConflictSerializable(schedule) {
  var graph = {};
  var t1 = pickXact("T1"),
      t2 = pickXact("T2");
  _.each(schedule, function(op1, idx1) {
    _.each(schedule, function(op2, idx2) {
      if (idx2 <= idx1) return;
      if (op1.xact != op2.xact &&
          op1.obj == op2.obj &&
          (op1.op == "W" || op2.op == "W")) {
        graph[op1.xact] = op2.xact;
      }
    })
  });
  return _.size(graph) <= 1;
}

function isSerializable(schedule, serialSchedule) {
  function isEqual(dbstate, s1, s2) {
    var res1 = execute(s1, _.clone(dbstate)),
        res2 = execute(s2, _.clone(dbstate));
    return _.isEqual(res1, res2);
  };
  var dbStates = _.times(10, function() {
    return genInitDB(schedule); });
  var t1 = pickXact(schedule, "T1"),
      t2 = pickXact(schedule, "T2");
  var ser1 = _.flatten([t1, t2]),
      ser2 = _.flatten([t2, t1]);
  var equals = _.map([ser1, ser2], function(ser) {
    return _.all(dbStates, function(state) {
      return isEqual(state, schedule, ser);
    });
  })
  return _.any(equals);
}

function isS2PL(schedule) {
  var locks = {};
  _.each(schedule, function(op) {
    locks[op.obj] = { xact: null, type: null };
  });

  function acquire(xact, obj, type) {
    if (locks[obj].xact == null) {
      locks[obj] = { xact: xact, type: type };
      return true;
    }
    if (locks[obj].xact == xact) {
      if (locks[obj].type == "X" && type == "S") {
      } else {
        locks[obj].type = type;
      }
      return true;
    }
    if (locks[obj].xact != xact && 
        !(locks[obj].type == "S" && type == "S")) {
      return false;
    }
    return true;
  }

  function isLastOp(xact, idx) {
    return _.size(_.filter(schedule, function(op, i) {
      return i > idx && op.xact == xact;
    })) == 0;
  }

  function releaseLocks(xact) {
    return _.each(locks, function(o, obj) {
      if (o.xact == xact) {
        o.xact = o.type = null;
      };
    });
  }

  return _.all(schedule, function(op, idx) {
    var type = (op.op == "W")? "X" : "S";
    if (!acquire(op.xact, op.obj, type)) {
      return false;
    }

    if (isLastOp(op.xact, idx)) {
      releaseLocks(op.xact);
    }
    return true;

  });
}

function findAnomalies(schedule) {
}

function scheduleToTable(schedule) {
   var table = _.times(2, function(idx) {
     return _.times(_.size(schedule), function() { 
       return {xact: "T"+(idx+1)}; 
     })
   });
   _.each(schedule, function(op, cidx) {
     var ridx = (op.xact == "T1")? 0 : 1;
     table[ridx][cidx] = op;
   });
   return table;
}

function genProblem(opts) {
  var oidx = 0;
  var schedule = null;
  var isCS = randItem([true, false]);
  var isSer = isCS || randItem([true, false]);

  while(oidx < 70) {
    oidx ++;
    var serial = genTransactions(opts);
    var idx = 0;

    while(idx < 500) {
      idx ++;
      var tmpschedule = genSchedule(serial);
      if (isConflictSerializable(tmpschedule) == isCS &&
          isSerializable(tmpschedule) == isSer) {
        schedule = tmpschedule;
        break;
      }
    }

    if (schedule != null) break;
  }
  if (!schedule) {
    $("#problem").html("<h1>Failed to generate problem :(</h1>");
    return;
  }

  var data = { 
    schedules: _.map([schedule], scheduleToTable),
    isSerializable: isSerializable(schedule),
    isConflictSerializable: isConflictSerializable(schedule),
    isS2PL: isS2PL(schedule)
  };
  $("#problem").html(
      genHTML("#schedule-template", data)
  );

  $("#solution").html(genHTML("#sol-template", data));

}

// helper to make using handlerbars easier
function genHTML(elid, data) {
  var source   = $(elid).html();
  var template = Handlebars.compile(source);
  return template(data);
}


