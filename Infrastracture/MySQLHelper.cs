﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using System.Data;

namespace Infrastracture
{
    public class MySQLHelper : IDisposable
    {
        private System.ComponentModel.Component components = new System.ComponentModel.Component();
        private bool disposedValue = false; // To detect redundant calls

        private Dictionary<string, object> _argMySQLParam = new Dictionary<string, object>();
        private StringBuilder _argMySQLCommand = new StringBuilder();
        private MySqlConnection conn;
        private MySqlTransaction trans;

        private MySqlCommand cmd
        {
            get
            {
                var _cmd = new MySqlCommand(_argMySQLCommand.ToString(), conn);

                if (_argMySQLParam != null)
                {
                    foreach (var param in _argMySQLParam)
                    {
                        _cmd.Parameters.AddWithValue(param.Key, param.Value);
                    }
                }

                _cmd.CommandTimeout = 1000;

                return _cmd;
            }
        }

        public StringBuilder ArgMySQLCommand
        {
            get
            {
                return _argMySQLCommand;
            }

            set
            {
                _argMySQLCommand = value;
            }
        }

        public Dictionary<string, object> ArgMysqlParam
        {
            get
            {
                return _argMySQLParam;
            }

            set
            {
                _argMySQLParam = value;
            }
        }

        public void BeginTransaction()
        {
            try
            {
                if (trans != null)
                {
                    trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);
                }
            }
            catch
            {

                throw;
            }
        }

        public MySQLHelper(string connString, StringBuilder argMySQLCommand = null, Dictionary<string, object> argMySQLParam = null)
        {
            try
            {
                conn = new MySqlConnection(connString);

                _argMySQLCommand = argMySQLCommand;
                _argMySQLParam = argMySQLParam;
            }
            catch
            {

                throw;
            }
        }

        public int ExecuteMySQL()
        {
            try
            {
                int result = 0;

                if (conn.State.Equals(ConnectionState.Closed))
                {
                    conn.Open();
                    trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);
                }

                result = cmd.ExecuteNonQuery();

                return result;
            }
            catch
            {
                trans.Rollback();
                throw;
            }
        }

        public void CommitTransaction()
        {
            try
            {
                trans.Commit();
            }
            catch
            {
                trans.Rollback();
                throw;
            }
            finally
            {
                conn.Close();
            }
        }

        public MySqlDataReader GetMySQLReader()
        {
            try
            {
                if (conn.State.Equals(ConnectionState.Closed))
                    conn.Open();

                return cmd.ExecuteReader();

            }
            catch
            {

                throw;
            }
        }

        public void RollbackTransaction()
        {
            try
            {
                trans.Rollback();
            }
            catch
            {

                throw;
            }
        }

        #region IDisposable Support
        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue && disposing)
            {
                try
                {
                    cmd.Dispose();
                    conn.Dispose();

                    if (trans != null)
                        trans.Dispose();
                }
                catch
                {

                }

                if (conn.State == ConnectionState.Open)
                    conn.Close();

                if (conn != null)
                    MySqlConnection.ClearPool(conn);

                components.Dispose();
            }

            disposedValue = true;
        }

        // TODO: override a finalizer only if Dispose(bool disposing) above has code to free unmanaged resources.
        ~MySQLHelper()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(false);
        }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
            // TODO: uncomment the following line if the finalizer is overridden above.
            GC.SuppressFinalize(this);
        }
        #endregion

    }
}
