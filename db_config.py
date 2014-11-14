# CPAC/GUI/interface/windows/db_config.py
#
# Author: Daniel Clark, 2014

# Import packages
import numpy as np
import pyximport
pyximport.install(setup_args={'include_dirs': [np.get_include()]})
from CPAC.GUI.interface.utils.generic_class import GenericClass
from CPAC.GUI.interface.utils.constants import control, dtype
import sys
import wx
ID_RUN_EXT = 11

# Database configuration window class
class DBConfig(wx.Frame):
    '''
    Class that inherits from wx.Frame and creates a database
    configuration window.
    '''

    # Init class method
    def __init__(self, parent):

        # Init object as a frame
        wx.Frame.__init__(self, parent, title='CPAC - Database configuration',
                          size = (820,450))
        # Init frame components
        mainSizer = wx.BoxSizer(wx.VERTICAL)
        self.panel = wx.Panel(self)
        self.window = wx.ScrolledWindow(self.panel)
        # Init page
        self.page = GenericClass(self.window, 'miNDAR Database connection')

        # Add textboxes
        # Username
        self.page.add(label='Username ',
                 control = control.TEXT_BOX,
                 name = 'username',
                 type = dtype.STR,
                 comment = 'miNDAR database username',
                 values ='',
                 style= wx.EXPAND | wx.ALL,
                 size = (532,-1))
        # Password
        self.page.add(label= 'Password ',
                 control = control.TEXT_BOX,
                 name = 'password',
                 type = dtype.STR,
                 comment = 'miNDAR database password',
                 values ='',
                 style= wx.EXPAND | wx.ALL,
                 size = (532,-1))
        # Hostname
        self.page.add(label= 'Hostname ',
                 control = control.TEXT_BOX,
                 name = 'hostname',
                 type = dtype.STR,
                 comment = 'miNDAR database hostname, typically in the form:\n'
                           'mindarvpc.{address_str}.{region}.rds.amazonaws.com',
                 values ='',
                 style= wx.EXPAND | wx.ALL,
                 size = (532,-1))
        # Port
        self.page.add(label= 'Port ',
                 control = control.TEXT_BOX,
                 name = 'port',
                 type = dtype.STR,
                 comment = 'Port number to access the database instance on.\n'
                           '\'1521\' by default',
                 values ='1521',
                 style= wx.EXPAND | wx.ALL,
                 size = (532,-1))
        # SID
        self.page.add(label= 'SID ',
                 control = control.TEXT_BOX,
                 name = 'sid',
                 type = dtype.STR,
                 comment = 'Site identifier for miNDAR database.\n'
                           '\'ORCL\' by default',
                 values ='ORCL',
                 style= wx.EXPAND | wx.ALL,
                 size = (532,-1))

        # Otherstuff
        self.page.set_sizer()

        mainSizer.Add(self.window, 1, wx.EXPAND)

        btnPanel = wx.Panel(self.panel, -1)
        hbox = wx.BoxSizer(wx.HORIZONTAL)

        self.multiscan = wx.CheckBox(btnPanel, -1, label = "Multiscan Data")

        if 'linux' in sys.platform:
            hbox.Add(self.multiscan,0, flag=wx.TOP, border=5)
        else:
            hbox.Add(self.multiscan, 0, flag=wx.RIGHT | wx.BOTTOM, border=5)

        buffer2 = wx.StaticText(btnPanel, label = "\t")
        hbox.Add(buffer2)

        run_ext = wx.Button(btnPanel, ID_RUN_EXT, "Generate Subject Lists", (280,10), wx.DefaultSize, 0 )
        #self.Bind(wx.EVT_BUTTON, lambda event: self.save(event,'run'), id=ID_RUN_EXT)
        hbox.Add( run_ext, 1, flag=wx.LEFT|wx.ALIGN_LEFT, border=10)

        buffer = wx.StaticText(btnPanel, label = "\t\t\t\t")
        hbox.Add(buffer)

        # Cancel button
        cancel = wx.Button(btnPanel, wx.ID_CANCEL, "Cancel",(220,10), wx.DefaultSize, 0 )
        self.Bind(wx.EVT_BUTTON, self.cancel, id=wx.ID_CANCEL)
        hbox.Add( cancel, 0, flag=wx.LEFT|wx.BOTTOM, border=5)

        load = wx.Button(btnPanel, wx.ID_ADD, "Load Settings", (280,10), wx.DefaultSize, 0 )
        #self.Bind(wx.EVT_BUTTON, self.load, id=wx.ID_ADD)
        hbox.Add(load, 0.6, flag=wx.LEFT|wx.BOTTOM, border=5)

        save = wx.Button(btnPanel, wx.ID_SAVE, "Save Settings", (280,10), wx.DefaultSize, 0 )
        #self.Bind(wx.EVT_BUTTON, lambda event: self.save(event,'save'), id=wx.ID_SAVE)
        hbox.Add(save, 0.6, flag=wx.LEFT|wx.BOTTOM, border=5)

        btnPanel.SetSizer(hbox)

        mainSizer.Add(btnPanel, 0.5,  flag=wx.ALIGN_RIGHT|wx.RIGHT, border=20)

        self.panel.SetSizer(mainSizer)

        self.Show()

    # Close the window if user hits cancel
    def cancel(self, event):
        self.Close()

# Launch window
app = wx.App(False)
DBConfig(None)
app.MainLoop()
