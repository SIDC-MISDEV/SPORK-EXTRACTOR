using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Sap.Data.Hana;
using System.Data;

namespace Infrastracture
{
    public class HanaSQLHelper : IDisposable
    {
        private System.ComponentModel.Component components = new System.ComponentModel.Component();
        private bool disposedValue = false; // To detect redundant calls

        private StringBuilder _argMySQLCommand = new StringBuilder();
        private Dictionary<string, object> _argMySQLParam = new Dictionary<string, object>();

        private HanaConnection conn;

        private HanaCommand cmd
        {
            get
            {
                var _cmd = new HanaCommand(_argMySQLCommand.ToString(), conn);

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

        public HanaSQLHelper(string connString, StringBuilder argMySQLCommand = null, Dictionary<string, object> argMySQLParam = null)
        {
            try
            {
                conn = new HanaConnection(connString);

                _argMySQLCommand = ArgMySQLCommand;
                _argMySQLParam = argMySQLParam;
            }
            catch
            {

                throw;
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

        public HanaDataReader GetMySQLReader()
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


        #region IDisposable Support

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue && disposing)
            {
                try
                {
                    cmd.Dispose();
                    conn.Dispose();
                }
                catch
                {

                    throw;
                }

                if (conn.State == ConnectionState.Open)
                    conn.Close();

                if (conn != null)
                    HanaConnection.ClearPool(conn);

                components.Dispose();

            }

            disposedValue = true;
        }

        // TODO: override a finalizer only if Dispose(bool disposing) above has code to free unmanaged resources.
        // ~HanaSQLHelper() {
        //   // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
        //   Dispose(false);
        // }

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
