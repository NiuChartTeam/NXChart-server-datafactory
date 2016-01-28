package com.niuchart.datafactory.command;

import java.util.List;

/**
 * Created by linxiaolong on 16/1/14.
 */
public interface NXKeyDimenCommand extends NXKeyCommand{
    List<? extends NXKeyCommand> getGranularities();
}
