package com.test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.junit.Test;
import org.mockito.Mockito;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.segmentreader.mapreduce.AbstractUserSegmentsMapper;
import com.segmentreader.mapreduce.UserModCommand;

public class MapperTest {
    
    @Test
    public void test() {
       AbstractUserSegmentsMapper testMapper = Mockito.mock(AbstractUserSegmentsMapper.class);
       
       List <String> list = new LinkedList<String>();
       LinkedList <String> spyList = (LinkedList<String>) spy(list);
       doReturn("iphone").when(spyList).get(0);
       
       Context contextMock = Mockito.mock(Context.class);
       UserModCommand userModCommand = new UserModCommand("1", "add", spyList);
     
       // testMapper.map(NullWritable.get(), userModCommand, contextMock);
 
    }

}
