/*
 *    _____      _ ____  ____
 *   / ___/_____(_) __ \/ __ )
 *   \__ \/ ___/ / / / / __  |
 *  ___/ / /__/ / /_/ / /_/ / 
 * /____/\___/_/_____/_____/  
 *
 *
 * BEGIN_COPYRIGHT
 *
 * This file is part of SciDB.
 * Copyright (C) 2008-2014 SciDB, Inc.
 *
 * SciDB is free software: you can redistribute it and/or modify
 * it under the terms of the AFFERO GNU General Public License as published by
 * the Free Software Foundation.
 *
 * SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
 * INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
 * NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
 * the AFFERO GNU General Public License for the complete license terms.
 *
 * You should have received a copy of the AFFERO GNU General Public License
 * along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
 *
 * END_COPYRIGHT
 */
#include "query/Operator.h"
//#include <log4cxx/logger.h>

namespace scidb
{

// Logger for operator. static to prevent visibility of variable outside of file
//static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.qproc.collate"));

class PhysicalCollate : public PhysicalOperator
{
public:
    PhysicalCollate(string const& logicalName,
                    string const& physicalName,
                    Parameters const& parameters,
                    ArrayDesc const& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {}

    class OutputWriter
    {
    private:
        shared_ptr<Array> _output;
        Coordinates _outputCellPosition;
        int64_t _chunkSizeColumn;
        int64_t _chunkSizeRow;
        int64_t _startRow;
        int64_t _lastChunk;
        shared_ptr<ArrayIterator> _outputArrayIterator;
        shared_ptr<ChunkIterator> _outputChunkIterator;
        Value _dval;

    public:
        OutputWriter(ArrayDesc const& schema, shared_ptr<Query>& query):
            _output(new MemArray(schema, query)),
            _outputCellPosition(2, -1),  // Make sure (-1,-1) is not a valid coordinate 
            _chunkSizeColumn(schema.getDimensions()[1].getChunkInterval()),
            _chunkSizeRow(schema.getDimensions()[0].getChunkInterval()),
            _startRow(schema.getDimensions()[0].getStart()),
            _outputArrayIterator(_output->getIterator(0)) // the chunk iterator is NULL at the start
        {
        }


        /* row[0] contains the row coordinate of the chunk to write to
         * val is the value to be written
         * query is the query
         */
        void writeValue(Coordinates row, Value const& val, shared_ptr<Query>& query)
        {
            if(_outputCellPosition[1]<0) _lastChunk = -1;
            if ((row[0]/_chunkSizeRow != _lastChunk) && (_outputCellPosition[1] < 1))
            {
               if(_lastChunk> -1)
               {
                   _outputChunkIterator->flush();   // Flush this chunk, we're done with it
                   _outputChunkIterator.reset();
               }
               // We're going to write to a new chunk
               _outputCellPosition[1] = 0;      // Set the column to zero
               _outputCellPosition[0] = row[0]; // Set the row
               _lastChunk = _outputCellPosition[0] / _chunkSizeRow;
               // Now initialize the chunk iterator
               _outputChunkIterator = _outputArrayIterator->newChunk(_outputCellPosition).getIterator(query, ChunkIterator::SEQUENTIAL_WRITE);   
            }
            _outputCellPosition[0] = row[0];  // Set the row
            _outputChunkIterator->setPosition(_outputCellPosition);
            _outputChunkIterator->writeItem(val);
            _outputCellPosition[1]++;         // Increment the column
            if(_outputCellPosition[1] >= _chunkSizeColumn)
            {
               _outputCellPosition[1] = 0;    // Reset the column
               ++_outputCellPosition[0]; // Increment the row
               if(_outputCellPosition[0] / _chunkSizeRow != _lastChunk)
               {
                   _outputChunkIterator->flush();   // Flush this chunk, we're done with it
                   _outputChunkIterator.reset();
                   _lastChunk = -1;  // avoid double flush and reset (see above)
               }
            }
        }

        shared_ptr<Array> finalize()
        {
            if(_outputChunkIterator)
            {
                _outputChunkIterator->flush();
                _outputChunkIterator.reset();
            }
            _outputArrayIterator.reset();
            return _output;
        }
    };

    virtual ArrayDistribution getOutputDistribution(vector<ArrayDistribution> const& inputDistributions,
                                                    vector<ArrayDesc> const& inputSchemas) const
    {
       return ArrayDistribution(psUndefined);
    }

    /**
      * [Optimizer API] Determine if operator changes result chunk distribution.
      * @param sourceSchemas shapes of all arrays that will given as inputs.
      * @return true if will changes output chunk distribution, false if otherwise
      */
    virtual bool changesDistribution(std::vector<ArrayDesc> const& sourceSchemas) const
    {
        return true;
    }

    shared_ptr< Array> execute(vector< shared_ptr< Array> >& inputArrays, shared_ptr<Query> query)
    {
        OutputWriter outputArrayWriter(_schema, query);
        shared_ptr<Array> inputArray = inputArrays[0];
        ArrayDesc const& inputSchema = inputArray->getArrayDesc();
        AttributeID const nAttrs = inputSchema.getAttributes(true).size();
        vector<string> attributeNames(nAttrs, "");
        vector<shared_ptr<ConstArrayIterator> > saiters(nAttrs);
        vector<shared_ptr<ConstChunkIterator> > sciters(nAttrs);
        for (AttributeID i = 0; i<nAttrs; ++i)
        {
            attributeNames[i] = inputSchema.getAttributes()[i].getName();
            saiters[i] = inputArray->getConstIterator(i);
        }

        while (!saiters[0]->end())
        {
            for (AttributeID i = 0; i<nAttrs; ++i)
            {
                vector<double> inputData;
                sciters[i] = saiters[i]->getChunk().getConstIterator(ChunkIterator::IGNORE_EMPTY_CELLS);
            }
            bool ok = true;
            while(ok)
            {
                ok = false;
                for (AttributeID i = 0; i<nAttrs; ++i)
                {
                    if(!sciters[i]->end())
                    {
                        Value const& val = sciters[i]->getItem();
                        Coordinates row = sciters[i]->getPosition();
                        outputArrayWriter.writeValue(row, val, query);
                        ++(*sciters[i]);
                    }
                    ok = ok || !sciters[i]->end();
                }
            }
            for (AttributeID i = 0; i<nAttrs; ++i)
            {
                ++(*saiters[i]);
            }
        }
        return outputArrayWriter.finalize();
    }
};
REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalCollate, "collate", "PhysicalCollate");

} //namespace scidb
