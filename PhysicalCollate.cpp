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
#include <log4cxx/logger.h>

#include <stdio.h>

namespace scidb
{

// Logger for operator. static to prevent visibility of variable outside of file
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.qproc.collate"));

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
        bool _initialWrite;
        int64_t _chunkSizeColumn;
        int64_t _chunkSizeRow;
        shared_ptr<ArrayIterator> _outputArrayIterator;
        shared_ptr<ChunkIterator> _outputChunkIterator;
        Value _dval;

    public:
        OutputWriter(ArrayDesc const& schema, shared_ptr<Query>& query):
            _output(new MemArray(schema, query)),
            _outputCellPosition(2, 0),    //--> {0,0}; //make sure output doesn't start at coord < 0
            _initialWrite(true),
            _chunkSizeColumn(schema.getDimensions()[1].getChunkInterval()),
            _chunkSizeRow(schema.getDimensions()[0].getChunkInterval()),
            _outputArrayIterator(_output->getIterator(0)) //the chunk iterator is NULL at the start
        {
// XXX initialize the chunk iterator here?
        }

        /**
         * if this is the first call
         *    initialize _outputChunkIterator
         *
         * else if we need to open a new chunk
         *    flush _outputChunkIterator
         *    set _outputChunkIterator to next chunk
         *
         * 
         * set position on _outputChunkIterator
         * increment coords
         * write
         */
        void writeValue(Value const& val, shared_ptr<Query>& query)
        {
            if (_initialWrite)
            {
               _outputChunkIterator = _outputArrayIterator->newChunk(_outputCellPosition).getIterator(query, ChunkIterator::SEQUENTIAL_WRITE);   
               _initialWrite = false;
            }
            else if(_outputCellPosition[0] % _chunkSizeRow == 0)
            {
               _outputChunkIterator->flush();
               _outputChunkIterator = _outputArrayIterator->newChunk(_outputCellPosition).getIterator(query, ChunkIterator::
SEQUENTIAL_WRITE);
            }
            _outputChunkIterator->setPosition(_outputCellPosition);
            _outputChunkIterator->writeItem(val);
            _outputCellPosition[1]++;
            if(_outputCellPosition[1] > _chunkSizeColumn)
            {
               _outputCellPosition[0]++;
               _outputCellPosition[1] = 0;
            }
/*
            Coordinates chunkPosition = _outputCellPosition;
            _output->getArrayDesc().getChunkPositionFor(chunkPosition);
            
            //true at the very first writeValue call AND true when we have a full chunk
            if (chunkPosition[1] != _outputChunkPosition[1])  //first chunk, or a new chunk
            {
                if (_outputChunkIterator)
                {
                    _outputChunkIterator->flush(); //flush the last chunk if any
                }
                _outputChunkPosition = chunkPosition;
                //open the new chunk
                _outputChunkIterator = _outputArrayIterator->newChunk(chunkPosition).getIterator(query, ChunkIterator::SEQUENTIAL_WRITE);
            }
            //write
            _dval.setDouble(val);
            _outputChunkIterator->setPosition(_outputCellPosition);
            _outputChunkIterator->writeItem(_dval);
            _outputCellPosition[1]++;
*/
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
// XXX Is this right? Should it be the distribution of the output array?
       return ArrayDistribution(psUndefined);
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

fprintf(stderr, "--------------\n");
        while (!saiters[0]->end())
        {
            for (AttributeID i = 0; i<nAttrs; ++i)
            {
                vector<double> inputData;
                sciters[i] = saiters[i]->getChunk().getConstIterator(ChunkIterator::IGNORE_EMPTY_CELLS);
                while( !sciters[i]->end())
                {
                    Value const& val = sciters[i]->getItem();
fprintf(stderr, "val %s = %f\n",attributeNames[i].c_str(),val.getDouble());

outputArrayWriter.writeValue(val, query);
                    ++(*sciters[i]);
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
