/*
 * Container.java
 *
 */

package com.broaddata.common.util;

public interface Container
{
    public void sayHello(String message);
    public Object getService(String serviceName);
    public Object getParameter(String parameterName);
}
