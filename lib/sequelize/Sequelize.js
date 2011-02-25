var
  Client = require('mysql').Client,
  Sequelize = function(database, username, password, options) {
  options = options || {}

  this.tables = {}
  this.options = Sequelize.Helper.Hash.without(options, ["host", "port", "disableTableNameModification"])
  this.config = {
    database: database,
    username: username,
    password: (((["", null, false].indexOf(password) > -1) || (typeof password == 'undefined')) ? null : password),
    host    : options.host || 'localhost',
    port    : options.port || 3306
  }

  Sequelize.Helper.configure({
    disableTableNameModification: (options.disableTableNameModification || false)
  })
}

var classMethods = {
  Helper: new (require(__dirname + "/Helper").Helper)(Sequelize),

  STRING: 'VARCHAR(255)',
  TEXT: 'TEXT',
  INTEGER: 'INT',
  DATE: 'DATETIME',
  BOOLEAN: 'TINYINT(1)',
  FLOAT: 'FLOAT',
  
  sqlQueryFor: function(command, values) {
    var query = null
    
    if(values.hasOwnProperty('fields') && Array.isArray(values.fields))
      values.fields = values.fields.map(function(field) { return ['', field, ''].join('`') }).join(", ")
    
    switch(command) {
      case 'create':
        query = "CREATE TABLE IF NOT EXISTS `%{table}` (%{fields})"
        break
      case 'drop':
        query = "DROP TABLE IF EXISTS `%{table}`"
        break
      case 'select':
        values.fields = values.fields || '*'
        
        query = "SELECT %{fields} FROM `%{table}`"
        
        if(values.where) {
          if(Sequelize.Helper.Hash.isHash(values.where))
            values.where = Sequelize.Helper.SQL.hashToWhereConditions(values.where)
            
          query += " WHERE %{where}"
         }
        if(values.order) query += " ORDER BY %{order}"
        if(values.group) query += " GROUP BY %{group}"
        if(values.limit) {
          if(values.offset) query += " LIMIT %{offset}, %{limit}"
          else query += " LIMIT %{limit}"
        }
        break
      case 'insert':
        query = "INSERT INTO `%{table}` (%{fields}) VALUES (%{values})"
        break
      case 'update':
        if(Sequelize.Helper.Hash.isHash(values.values))
          values.values = Sequelize.Helper.SQL.hashToWhereConditions(values.values)
      
        query = "UPDATE `%{table}` SET %{values} WHERE `id`=%{id}"
        break
      case 'delete':
        if(Sequelize.Helper.Hash.isHash(values.where))
          values.where = Sequelize.Helper.SQL.hashToWhereConditions(values.where)
          
        query = "DELETE FROM `%{table}` WHERE %{where}"
        if(typeof values.limit == 'undefined') query += " LIMIT 1"
        else if(values.limit != null) query += " LIMIT " + values.limit
        
        break
    }
    
    return Sequelize.Helper.evaluateTemplate(query, values)
  },
  chainQueries: function() {
    this.Helper.QueryChainer.chain.apply(this.Helper.QueryChainer, arguments)
  }
}

Sequelize.prototype = {
  define: function(name, attributes, options) {
    var SequelizeTable = require(__dirname + "/SequelizeTable").SequelizeTable
    var _attributes = {}
    
    Sequelize.Helper.Hash.forEach(attributes, function(value, key) {
      if(typeof value == 'string')
        _attributes[key] = { type: value }
      else if((typeof value == 'object') && (!value.length))
        _attributes[key] = value
      else
        throw new Error("Please specify a datatype either by using Sequelize.* or pass a hash!")
    })
    
    _attributes.createdAt = { type: Sequelize.DATE, allowNull: false}
    _attributes.updatedAt = { type: Sequelize.DATE, allowNull: false}
    
    var table = new SequelizeTable(Sequelize, this, Sequelize.Helper.SQL.asTableName(name), _attributes, options)
    
    // refactor this to use the table's attributes
    this.tables[name] = {klass: table, attributes: attributes}

    table.sequelize = this
    return table
  },
  
  import: function(path) {
    var imported  = require(path),
        self      = this,
        result    = {} 
    
    Sequelize.Helper.Hash.forEach(imported, function(definition, functionName) {
      definition(Sequelize, self)
    })
    
    Sequelize.Helper.Hash.forEach(this.tables, function(constructor, name) {
      result[name] = constructor.klass
    })

    return result
  },
  
  get tableNames() {
    var result = []
    Sequelize.Helper.Hash.keys(this.tables).forEach(function(tableName) {
      result.push(Sequelize.Helper.SQL.asTableName(tableName))
    })
    return result
  },
  
  sync: function(callback) {
    var finished = [],
        tables = this.tables,
        errors = []

    Sequelize.Helper.Hash.forEach(tables, function(table) {
      table.klass.prepareAssociations()
    })

    if((Sequelize.Helper.Hash.keys(this.tables).length == 0) && callback)
      callback()
    else
      Sequelize.Helper.Hash.forEach(tables, function(table) {
        table.klass.sync(function(_, err) {
          finished.push(true)
          if(err) errors.push(err)
          if((finished.length == Sequelize.Helper.Hash.keys(tables).length) && callback)
            callback(errors)
        })
      })
  },
  
  drop: function(callback) {
    var finished = [],
        tables = this.tables,
        errors = []

    if((Sequelize.Helper.Hash.keys(tables).length == 0) && callback)
      callback()
    else
      Sequelize.Helper.Hash.forEach(tables, function(table, tableName) {
        table.klass.drop(function(_, err) {
          finished.push(true)
          if(err) errors.push(err)
          if((finished.length == Sequelize.Helper.Hash.keys(tables).length) && callback)
            callback(errors)
        })
      })
  },
  
  query: function(queryString, callback) {
    var self   = this, client = self.client;
    if(client&&client.connected) {
      onAuth(null);
    } else {
      client = new Client();
      client.user     = self.config.username;
      client.password = self.config.password;
      client.host     = self.config.host;
      client.database = self.config.database;
      client.port     = self.config.port;
      client.connect(onAuth);
      self.client = client;
    }
    
    function onAuth( err ) {
      if(err)
        return callback([], null, err);

      if(!self.options.disableLogging)
        Sequelize.Helper.log("Executing the query: " + queryString)
      
      client.query(queryString,function(err, rows, fields){
            function cb() {
              if(err)
                return Sequelize.Helper.log(err);
              if(callback) {
                if(rows instanceof Array)
                  callback(rows, null);
                else
                  callback([], rows);
              }
            }
            if(!self.options.disconnectManually)
                self.end(cb);
            else
                cb();
        });
    }
  },
  
  end : function( callback ) {
      var c = this.client;
      this.client = null;
      if(c&&c.connected) {
        c.end(callback);
      } else
        callback();
  }
}

for (var key in classMethods) Sequelize[key] = classMethods[key]

exports.Sequelize = Sequelize